import sqlite3
import logging
from datetime import datetime, timedelta
import random
import time

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_name='worker_bot.db'):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.init_db()
    
    def init_db(self):
        """Инициализация базы данных"""
        # Таблица пользователей - добавляем поля для наставников
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            total_earned REAL DEFAULT 0,
            team_count INTEGER DEFAULT 0,
            worker_percent INTEGER DEFAULT 70,
            hide_from_top BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_active_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT 1,
            days_in_team INTEGER DEFAULT 0,
            profits_count INTEGER DEFAULT 0,
            is_blocked BOOLEAN DEFAULT 0,
            mentor_id INTEGER DEFAULT NULL,
            is_mentor BOOLEAN DEFAULT 0,
            mentor_description TEXT DEFAULT NULL
        )
        ''')
        
        # Таблица кошельков пользователей
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_wallets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            wallet_address TEXT,
            wallet_type TEXT DEFAULT 'TON Wallet',
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
        ''')
        
        # Таблица заявок на вывод - с новыми направлениями
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount REAL DEFAULT 0,
            wallet_address TEXT,
            wallet_type TEXT,
            direction TEXT,
            status TEXT DEFAULT 'pending',
            screenshot_path TEXT,
            admin_comment TEXT,
            gift_url TEXT,
            worker_percent INTEGER,
            admin_amount REAL DEFAULT 0,
            worker_amount REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Таблица изображений для пруфов
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS proof_images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            withdrawal_id INTEGER,
            file_id TEXT,
            file_type TEXT,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Таблица статистики команды - убраны team_members_count
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS team_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            total_amount REAL DEFAULT 0,
            total_profits INTEGER DEFAULT 0,
            today_amount REAL DEFAULT 0,
            today_profits INTEGER DEFAULT 0,
            most_common_direction TEXT,
            active_workers INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Таблица админов
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS admins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE,
            username TEXT,
            first_name TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Инициализация статистики команды
        self.cursor.execute("SELECT COUNT(*) FROM team_stats")
        if self.cursor.fetchone()[0] == 0:
            today = datetime.now().date()
            
            # Общая сумма и количество профитов
            self.cursor.execute('''SELECT SUM(amount), COUNT(*) FROM withdrawals WHERE status = 'paid' ''')
            total_stats = self.cursor.fetchone()
            total_amount = total_stats[0] or 0
            total_profits = total_stats[1] or 0
            
            # Сегодняшние данные
            self.cursor.execute('''SELECT SUM(amount), COUNT(*) FROM withdrawals WHERE status = 'paid' AND DATE(created_at) = ?''', (today,))
            today_stats = self.cursor.fetchone()
            today_amount = today_stats[0] or 0
            today_profits = today_stats[1] or 0
            
            # Самое популярное направление
            self.cursor.execute('''SELECT direction, COUNT(*) as count FROM withdrawals WHERE status = 'paid' GROUP BY direction ORDER BY count DESC LIMIT 1''')
            direction_result = self.cursor.fetchone()
            most_common_direction = direction_result[0] if direction_result else "Нет данных"
            
            # Активные воркеры
            self.cursor.execute("SELECT COUNT(*) FROM users WHERE is_active = 1 AND is_blocked = 0")
            active_workers = self.cursor.fetchone()[0] or 0
            
            self.cursor.execute('''
            INSERT INTO team_stats (total_amount, total_profits, today_amount, today_profits, 
                                   most_common_direction, active_workers)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (total_amount, total_profits, today_amount, today_profits, 
                  most_common_direction, active_workers))
        
        self.conn.commit()
        logger.info("База данных инициализирована")
    
    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ ==========
    
    def create_or_update_user(self, user_id, username, first_name, last_name):
        """Создает или обновляет пользователя с правильным расчетом времени в тиме"""
        try:
            # Проверяем, существует ли пользователь
            self.cursor.execute('''SELECT created_at, is_mentor, mentor_description FROM users WHERE user_id = ?''', (user_id,))
            existing_user = self.cursor.fetchone()
            
            if existing_user:
                # Пользователь существует - обновляем время активности
                join_date = datetime.strptime(existing_user[0], '%Y-%m-%d %H:%M:%S')
                current_date = datetime.now()
                
                # Рассчитываем дни в тиме
                days_in_team = (current_date - join_date).days
                if days_in_team < 1:
                    days_in_team = 1
                
                # Сохраняем статус наставника и описание
                is_mentor = existing_user[1]
                mentor_description = existing_user[2]
                
                # Обновляем данные пользователя
                self.cursor.execute('''
                UPDATE users 
                SET username = ?, first_name = ?, last_name = ?, 
                    last_active_at = CURRENT_TIMESTAMP, days_in_team = ?,
                    is_mentor = ?, mentor_description = ?
                WHERE user_id = ?
                ''', (username, first_name, last_name, days_in_team, is_mentor, mentor_description, user_id))
                
                logger.info(f"Обновлен пользователь {user_id}: {days_in_team} дней в тиме")
            else:
                # Новый пользователь - создаем запись с процентом 70%
                self.cursor.execute('''
                INSERT INTO users 
                (user_id, username, first_name, last_name, worker_percent, days_in_team) 
                VALUES (?, ?, ?, ?, 70, 1)
                ''', (user_id, username, first_name, last_name))
                
                logger.info(f"Создан новый пользователь {user_id}")
            
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка создания/обновления пользователя: {e}")
            return False
    
    def get_user(self, user_id):
        self.cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return self.cursor.fetchone()
    
    def get_user_stats(self, user_id):
        self.cursor.execute('''
        SELECT username, first_name, last_name,
               total_earned, team_count, worker_percent, is_active,
               hide_from_top, days_in_team, profits_count, mentor_id, is_mentor, mentor_description
        FROM users WHERE user_id = ?
        ''', (user_id,))
        return self.cursor.fetchone()
    
    def get_user_profit_stats(self, user_id):
        """Получает расширенную статистику профитов пользователя"""
        # Общая сумма и количество профитов
        self.cursor.execute('''
        SELECT SUM(amount), COUNT(*) 
        FROM withdrawals 
        WHERE user_id = ? AND status = 'paid'
        ''', (user_id,))
        total_stats = self.cursor.fetchone()
        total_earned_ton = total_stats[0] or 0
        total_profits = total_stats[1] or 0
        
        # Средний профит
        avg_profit = total_earned_ton / total_profits if total_profits > 0 else 0
        
        # Рекордный профит
        self.cursor.execute('''
        SELECT MAX(amount) 
        FROM withdrawals 
        WHERE user_id = ? AND status = 'paid'
        ''', (user_id,))
        max_profit_result = self.cursor.fetchone()
        max_profit = max_profit_result[0] or 0
        
        # За последние 7 дней
        week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        self.cursor.execute('''
        SELECT SUM(amount) 
        FROM withdrawals 
        WHERE user_id = ? AND status = 'paid' AND DATE(created_at) >= ?
        ''', (user_id, week_ago))
        week_profit_result = self.cursor.fetchone()
        week_profit = week_profit_result[0] or 0
        
        # За последние 30 дней
        month_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.cursor.execute('''
        SELECT SUM(amount) 
        FROM withdrawals 
        WHERE user_id = ? AND status = 'paid' AND DATE(created_at) >= ?
        ''', (user_id, month_ago))
        month_profit_result = self.cursor.fetchone()
        month_profit = month_profit_result[0] or 0
        
        # За последние 180 дней
        half_year_ago = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
        self.cursor.execute('''
        SELECT SUM(amount) 
        FROM withdrawals 
        WHERE user_id = ? AND status = 'paid' AND DATE(created_at) >= ?
        ''', (user_id, half_year_ago))
        half_year_profit_result = self.cursor.fetchone()
        half_year_profit = half_year_profit_result[0] or 0
        
        return (total_earned_ton, total_profits, avg_profit, max_profit, 
                week_profit, month_profit, half_year_profit)
    
    def get_user_period_profit(self, user_id, days):
        """Получает сумму профитов пользователя за указанный период"""
        period_ago = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        self.cursor.execute('''
        SELECT SUM(amount) 
        FROM withdrawals 
        WHERE user_id = ? AND status = 'paid' AND DATE(created_at) >= ?
        ''', (user_id, period_ago))
        result = self.cursor.fetchone()
        return result[0] or 0
    
    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С КОШЕЛЬКАМИ ==========
    
    def add_wallet(self, user_id, wallet_address, wallet_type="TON Wallet"):
        """Добавляет кошелек пользователю"""
        try:
            # Деактивируем все предыдущие кошельки
            self.cursor.execute('''
            UPDATE user_wallets SET is_active = 0 WHERE user_id = ?
            ''', (user_id,))
            
            # Добавляем новый кошелек как активный
            self.cursor.execute('''
            INSERT INTO user_wallets (user_id, wallet_address, wallet_type, is_active)
            VALUES (?, ?, ?, 1)
            ''', (user_id, wallet_address, wallet_type))
            
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления кошелька: {e}")
            return False
    
    def get_user_wallets(self, user_id):
        """Получает все кошельки пользователя"""
        self.cursor.execute('''
        SELECT id, wallet_address, wallet_type, is_active, created_at 
        FROM user_wallets 
        WHERE user_id = ? 
        ORDER BY is_active DESC, created_at DESC
        ''', (user_id,))
        return self.cursor.fetchall()
    
    def get_active_wallet(self, user_id):
        """Получает активный кошелек пользователя"""
        self.cursor.execute('''
        SELECT wallet_address, wallet_type 
        FROM user_wallets 
        WHERE user_id = ? AND is_active = 1
        LIMIT 1
        ''', (user_id,))
        return self.cursor.fetchone()
    
    def set_active_wallet(self, user_id, wallet_id):
        """Устанавливает кошелек как активный"""
        try:
            # Деактивируем все кошельки
            self.cursor.execute('''
            UPDATE user_wallets SET is_active = 0 WHERE user_id = ?
            ''', (user_id,))
            
            # Активируем выбранный
            self.cursor.execute('''
            UPDATE user_wallets SET is_active = 1 
            WHERE user_id = ? AND id = ?
            ''', (user_id, wallet_id))
            
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка активации кошелька: {e}")
            return False
    
    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С ПРОФИЛЕМ ==========
    
    def update_total_earned(self, user_id, amount):
        self.cursor.execute('''
        UPDATE users 
        SET total_earned = total_earned + ?,
            profits_count = profits_count + 1
        WHERE user_id = ?
        ''', (amount, user_id))
        self.conn.commit()
        
        self.update_team_stats(amount)
    
    def update_worker_percent(self, user_id, percent):
        self.cursor.execute('''
        UPDATE users 
        SET worker_percent = ?
        WHERE user_id = ?
        ''', (percent, user_id))
        self.conn.commit()
    
    def toggle_hide_from_top(self, user_id):
        self.cursor.execute('''
        UPDATE users 
        SET hide_from_top = NOT hide_from_top
        WHERE user_id = ?
        ''', (user_id,))
        self.conn.commit()
        
        self.cursor.execute('''
        SELECT hide_from_top FROM users WHERE user_id = ?
        ''', (user_id,))
        return self.cursor.fetchone()[0]
    
    def get_top_workers(self, limit=10, exclude_hidden=True):
        if exclude_hidden:
            self.cursor.execute('''
            SELECT user_id, username, first_name, total_earned, profits_count
            FROM users 
            WHERE hide_from_top = 0 AND is_blocked = 0 AND total_earned > 0
            ORDER BY total_earned DESC 
            LIMIT ?
            ''', (limit,))
        else:
            self.cursor.execute('''
            SELECT user_id, username, first_name, total_earned, profits_count
            FROM users 
            WHERE is_blocked = 0 AND total_earned > 0
            ORDER BY total_earned DESC 
            LIMIT ?
            ''', (limit,))
        return self.cursor.fetchall()
    
    def get_user_rank(self, user_id):
        self.cursor.execute('''
        SELECT COUNT(*) + 1 as rank
        FROM users u1
        WHERE u1.total_earned > (
            SELECT u2.total_earned 
            FROM users u2 
            WHERE u2.user_id = ?
        ) AND u1.hide_from_top = 0 AND u1.is_blocked = 0
        ''', (user_id,))
        result = self.cursor.fetchone()
        return result[0] if result else 1
    
    def get_all_users(self):
        self.cursor.execute('''
        SELECT user_id, username, first_name, total_earned, worker_percent, is_active, is_blocked
        FROM users ORDER BY created_at DESC
        ''')
        return self.cursor.fetchall()
    
    def get_all_active_users(self):
        self.cursor.execute('''
        SELECT user_id, username, first_name 
        FROM users 
        WHERE is_active = 1 AND is_blocked = 0
        ''')
        return self.cursor.fetchall()
    
    def find_user_by_username(self, username):
        self.cursor.execute('''
        SELECT user_id, username, first_name, worker_percent, total_earned
        FROM users WHERE username LIKE ? AND is_blocked = 0
        ''', (f'%{username}%',))
        return self.cursor.fetchall()
    
    # ========== МЕТОДЫ ДЛЯ БЛОКИРОВКИ ==========
    
    def block_user(self, user_id):
        try:
            self.cursor.execute('''
            UPDATE users 
            SET is_blocked = 1 
            WHERE user_id = ?
            ''', (user_id,))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка блокировки пользователя: {e}")
            return False
    
    def unblock_user(self, user_id):
        try:
            self.cursor.execute('''
            UPDATE users 
            SET is_blocked = 0 
            WHERE user_id = ?
            ''', (user_id,))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка разблокировки пользователя: {e}")
            return False
    
    def is_user_blocked(self, user_id):
        self.cursor.execute('''
        SELECT is_blocked FROM users WHERE user_id = ?
        ''', (user_id,))
        result = self.cursor.fetchone()
        return result[0] if result else False
    
    def get_blocked_users(self):
        self.cursor.execute('''
        SELECT user_id, username, first_name 
        FROM users 
        WHERE is_blocked = 1
        ''')
        return self.cursor.fetchall()
    
    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С НАПРАВЛЕНИЯМИ ==========
    
    def get_most_common_direction(self, user_id):
        """Получает самое частое направление у пользователя"""
        self.cursor.execute('''
        SELECT direction, COUNT(*) as count 
        FROM withdrawals 
        WHERE user_id = ? AND status = 'paid'
        GROUP BY direction 
        ORDER BY count DESC 
        LIMIT 1
        ''', (user_id,))
        result = self.cursor.fetchone()
        return result[0] if result else None
    
    def get_global_most_common_direction(self):
        """Получает самое популярное направление среди всех пользователей"""
        self.cursor.execute('''
        SELECT direction, COUNT(*) as count 
        FROM withdrawals 
        WHERE status = 'paid'
        GROUP BY direction 
        ORDER BY count DESC 
        LIMIT 1
        ''')
        result = self.cursor.fetchone()
        return result[0] if result else "Нет данных"
    
    # ========== МЕТОДЫ ДЛЯ СТАТИСТИКИ КОМАНДЫ ==========
    
    def update_team_stats(self, amount):
        today = datetime.now().date()
        
        # Обновляем общую статистику
        self.cursor.execute('''
        UPDATE team_stats 
        SET total_amount = total_amount + ?,
            total_profits = total_profits + 1,
            updated_at = CURRENT_TIMESTAMP
        ''', (amount,))
        
        # Обновляем сегодняшнюю статистику
        self.cursor.execute('''
        SELECT COUNT(*) FROM withdrawals 
        WHERE DATE(created_at) = ? AND status = 'paid'
        ''', (today,))
        today_count = self.cursor.fetchone()[0]
        
        if today_count > 0:
            self.cursor.execute('''
            UPDATE team_stats 
            SET today_amount = today_amount + ?,
                today_profits = today_profits + 1
            ''', (amount,))
        
        # Обновляем самое популярное направление
        most_common_direction = self.get_global_most_common_direction()
        self.cursor.execute('''
        UPDATE team_stats 
        SET most_common_direction = ?
        ''', (most_common_direction,))
        
        # Обновляем количество активных воркеров
        self.cursor.execute("SELECT COUNT(*) FROM users WHERE is_active = 1 AND is_blocked = 0")
        active_workers = self.cursor.fetchone()[0] or 0
        self.cursor.execute('''
        UPDATE team_stats 
        SET active_workers = ?
        ''', (active_workers,))
        
        self.conn.commit()
    
    def get_real_team_stats(self):
        """Получает реальную статистику команды"""
        self.cursor.execute('''
        SELECT total_amount, total_profits, today_amount, today_profits, 
               most_common_direction, active_workers
        FROM team_stats LIMIT 1
        ''')
        stats = self.cursor.fetchone()
        
        if not stats:
            return (0, 0, 0, 0, "Нет данных", 0)
        
        return stats
    
    def get_real_team_stats_without_members(self):
        """Получает статистику команды без количества участников"""
        stats = self.get_real_team_stats()
        return stats
    
    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С НАСТАВНИКАМИ ==========
    
    def is_mentor(self, user_id):
        """Проверяет, является ли пользователь наставником"""
        self.cursor.execute('''
        SELECT is_mentor FROM users WHERE user_id = ?
        ''', (user_id,))
        result = self.cursor.fetchone()
        return result[0] if result else False
    
    def add_mentor(self, user_id, username, first_name, description):
        """Добавляет наставника"""
        try:
            # Проверяем, существует ли пользователь
            self.cursor.execute('''SELECT user_id FROM users WHERE user_id = ?''', (user_id,))
            existing_user = self.cursor.fetchone()
            
            if existing_user:
                self.cursor.execute('''
                UPDATE users 
                SET is_mentor = 1, mentor_description = ?
                WHERE user_id = ?
                ''', (description, user_id))
            else:
                self.cursor.execute('''
                INSERT INTO users (user_id, username, first_name, is_mentor, mentor_description, worker_percent, days_in_team)
                VALUES (?, ?, ?, 1, ?, 70, 1)
                ''', (user_id, username, first_name, description))
            
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления наставника: {e}")
            return False
    
    def remove_mentor(self, user_id):
        """Удаляет наставника"""
        try:
            self.cursor.execute('''
            UPDATE users 
            SET is_mentor = 0, mentor_description = NULL
            WHERE user_id = ?
            ''', (user_id,))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка удаления наставника: {e}")
            return False
    
    def get_all_mentors_with_info(self):
        """Получает всех наставников с информацией"""
        self.cursor.execute('''
        SELECT user_id, username, first_name, mentor_description
        FROM users 
        WHERE is_mentor = 1 AND is_blocked = 0
        ORDER BY first_name
        ''')
        return self.cursor.fetchall()
    
    def get_mentor_info(self, mentor_id):
        """Получает информацию о наставнике"""
        self.cursor.execute('''
        SELECT user_id, username, first_name, mentor_description
        FROM users 
        WHERE user_id = ? AND is_mentor = 1
        ''', (mentor_id,))
        return self.cursor.fetchone()
    
    def get_mentors_count(self):
        """Получает количество наставников"""
        self.cursor.execute('''
        SELECT COUNT(*) FROM users WHERE is_mentor = 1
        ''')
        return self.cursor.fetchone()[0]
    
    def get_user_mentor(self, user_id):
        """Получает ID наставника пользователя"""
        self.cursor.execute('''
        SELECT mentor_id FROM users WHERE user_id = ?
        ''', (user_id,))
        result = self.cursor.fetchone()
        return result[0] if result else None
    
    def set_user_mentor(self, user_id, mentor_id):
        """Устанавливает наставника для пользователя"""
        try:
            self.cursor.execute('''
            UPDATE users 
            SET mentor_id = ?
            WHERE user_id = ?
            ''', (mentor_id, user_id))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка установки наставника: {e}")
            return False
    
    def remove_user_mentor(self, user_id):
        """Удаляет наставника у пользователя"""
        try:
            self.cursor.execute('''
            UPDATE users 
            SET mentor_id = NULL
            WHERE user_id = ?
            ''', (user_id,))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления наставника: {e}")
            return False
    
    def get_mentor_students(self, mentor_id):
        """Получает список учеников наставника"""
        self.cursor.execute('''
        SELECT user_id, username, first_name
        FROM users 
        WHERE mentor_id = ? AND is_blocked = 0
        ORDER BY first_name
        ''', (mentor_id,))
        return self.cursor.fetchall()
    
    def get_mentor_students_count(self, mentor_id):
        """Получает количество учеников наставника"""
        self.cursor.execute('''
        SELECT COUNT(*) FROM users WHERE mentor_id = ?
        ''', (mentor_id,))
        return self.cursor.fetchone()[0]
    
    def get_mentor_students_total_profit(self, mentor_id):
        """Получает общую сумму профитов учеников наставника"""
        self.cursor.execute('''
        SELECT SUM(total_earned) FROM users WHERE mentor_id = ?
        ''', (mentor_id,))
        result = self.cursor.fetchone()
        return result[0] or 0
    
    def get_mentor_top_student(self, mentor_id):
        """Получает топ ученика наставника"""
        self.cursor.execute('''
        SELECT user_id, username, first_name, total_earned
        FROM users 
        WHERE mentor_id = ? AND is_blocked = 0
        ORDER BY total_earned DESC
        LIMIT 1
        ''', (mentor_id,))
        return self.cursor.fetchone()
    
    def get_students_with_mentors_count(self):
        """Получает количество учеников с наставниками"""
        self.cursor.execute('''
        SELECT COUNT(*) FROM users WHERE mentor_id IS NOT NULL
        ''')
        return self.cursor.fetchone()[0]
    
    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С ЗАЯВКАМИ ==========
    
    def create_withdrawal_with_url(self, user_id, amount, wallet_address, wallet_type, direction, gift_url, worker_percent):
        """Создает заявку на вывод со ссылкой на гифты"""
        worker_amount = (amount * worker_percent) / 100
        admin_amount = (amount * (100 - worker_percent)) / 100
        
        self.cursor.execute('''
        INSERT INTO withdrawals 
        (user_id, amount, wallet_address, wallet_type, direction, gift_url, worker_percent, admin_amount, worker_amount, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
        ''', (user_id, amount, wallet_address, wallet_type, direction, gift_url, worker_percent, admin_amount, worker_amount))
        
        withdrawal_id = self.cursor.lastrowid
        
        self.conn.commit()
        return withdrawal_id
    
    def update_withdrawal_amount(self, withdrawal_id, amount, worker_amount, admin_amount):
        """Обновляет сумму в заявке"""
        self.cursor.execute('''
        UPDATE withdrawals 
        SET amount = ?, worker_amount = ?, admin_amount = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        ''', (amount, worker_amount, admin_amount, withdrawal_id))
        self.conn.commit()
    
    def get_withdrawal(self, withdrawal_id):
        self.cursor.execute("SELECT * FROM withdrawals WHERE id = ?", (withdrawal_id,))
        return self.cursor.fetchone()
    
    def get_user_withdrawals(self, user_id):
        self.cursor.execute('''
        SELECT id, amount, direction, wallet_type, status, gift_url, worker_percent, worker_amount, admin_amount, created_at 
        FROM withdrawals 
        WHERE user_id = ? 
        ORDER BY created_at DESC
        ''', (user_id,))
        return self.cursor.fetchall()
    
    def update_withdrawal_status(self, withdrawal_id, status, admin_comment=None):
        if admin_comment:
            self.cursor.execute('''
            UPDATE withdrawals 
            SET status = ?, admin_comment = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''', (status, admin_comment, withdrawal_id))
        else:
            self.cursor.execute('''
            UPDATE withdrawals 
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''', (status, withdrawal_id))
        
        self.conn.commit()
    
    def get_pending_withdrawals(self):
        self.cursor.execute('''
        SELECT w.*, u.username, u.first_name, u.worker_percent
        FROM withdrawals w
        LEFT JOIN users u ON w.user_id = u.user_id
        WHERE w.status = 'pending'
        ORDER BY w.created_at
        ''')
        return self.cursor.fetchall()
    
    def add_proof_image(self, withdrawal_id, file_id, file_type):
        self.cursor.execute('''
        INSERT INTO proof_images (withdrawal_id, file_id, file_type)
        VALUES (?, ?, ?)
        ''', (withdrawal_id, file_id, file_type))
        self.conn.commit()
    
    def get_proof_images(self, withdrawal_id):
        self.cursor.execute('''
        SELECT file_id, file_type FROM proof_images 
        WHERE withdrawal_id = ?
        ORDER BY uploaded_at
        ''', (withdrawal_id,))
        return self.cursor.fetchall()
    
    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С АДМИНАМИ ==========
    
    def is_admin(self, user_id):
        """Проверяет, является ли пользователь админом"""
        self.cursor.execute('''
        SELECT COUNT(*) FROM admins WHERE user_id = ?
        ''', (user_id,))
        result = self.cursor.fetchone()
        return result[0] > 0 if result else False
    
    def add_admin(self, user_id, username, first_name):
        """Добавляет админа в базу данных"""
        try:
            self.cursor.execute('''
            INSERT OR IGNORE INTO admins (user_id, username, first_name)
            VALUES (?, ?, ?)
            ''', (user_id, username, first_name))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления админа: {e}")
            return False
    
    def remove_admin(self, user_id):
        """Удаляет админа из базы данных"""
        try:
            # Не даем удалить главного админа
            if user_id == 1034932955:  # ADMIN_ID
                return False
                
            self.cursor.execute('''
            DELETE FROM admins WHERE user_id = ?
            ''', (user_id,))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка удаления админа: {e}")
            return False
    
    def get_all_admins(self):
        """Получает список всех админов (только ID)"""
        self.cursor.execute('''
        SELECT user_id FROM admins
        ''')
        return [row[0] for row in self.cursor.fetchall()]
    
    def get_all_admins_with_info(self):
        """Получает список всех админов с информацией"""
        self.cursor.execute('''
        SELECT user_id, username, first_name, added_at FROM admins
        ORDER BY added_at
        ''')
        return self.cursor.fetchall()
    
    def get_admin_info(self, admin_id):
        """Получает информацию об админе"""
        self.cursor.execute('''
        SELECT user_id, username, first_name FROM admins WHERE user_id = ?
        ''', (admin_id,))
        return self.cursor.fetchone()
    
    def close(self):
        self.conn.close()

# Экспортируем класс Database
__all__ = ['Database']