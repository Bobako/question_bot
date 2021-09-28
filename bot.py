import datetime
import json
import asyncio

import telebot
from telebot.types import ReplyKeyboardRemove as RemoveMarkup
from telebot import types
from sqlalchemy import exc

from database import database_handler
import bot_config as cfg

db = database_handler.Handler("database/db.db")
bot = telebot.TeleBot(cfg.TOKEN)


@bot.message_handler(commands=["help", "start"])
def start_help(message):
    if message.chat.id < 0:
        bot.send_message(message.chat.id, text="Бот для рассылки опросов\nПожалуйста, перейдите в ЛС бота и "
                                               "напишите /join, чтобы разрешить отправлять вам сообщения.")
        return
    bot.send_message(message.chat.id, text=cfg.HELP_MSG, reply_markup=get_help_keyboard())


@bot.message_handler(commands=["chat_id"])
def get_chat_id(message):
    print(message.chat.id)
    bot.send_message(message.chat.id, text=str(message.chat.id))


@bot.message_handler(commands=["join"])
def start_user(message):
    if message.chat.id < 0:
        bot.reply_to(message, "Для регистрации в системе необходимо написать именно в личные сообщения боту")
        return
    username = msg_user_to_username(message.from_user)
    tg_user_id = message.from_user.id
    try:
        db.create_user(tg_user_id, message.from_user.username, username)
    except exc.IntegrityError:  # Уже был добавлен
        pass
    bot.send_message(message.from_user.id, "Вы были добавлены в систему")


@bot.message_handler(commands=["status"])
def status(message):
    if message.chat.id < 0:
        bot.reply_to(message, "Функционал доступен в только в ЛС")
        return
    try:
        user = db.get_user(message.from_user.id)
    except exc.NoResultFound:
        bot.send_message(message.from_user.id, "Вы не добавлены в систему")
    else:
        bot.send_message(message.from_user.id,
                         f"{('Роли: ' + ', '.join(user.get_roles()) if user.get_roles() else 'Нет ролей')}; "
                         f"{('Администратор.' if user.admin else '')}")



@bot.message_handler(commands=["admin"])
def start_admin(message):
    if message.chat.id < 0:
        bot.reply_to(message, "Функционал администратора доступен только в ЛС бота")
        return
    if message.from_user.username not in cfg.admins:
        bot.send_message(message.from_user.id, "Вы не являетесь администратором")
        return
    bot.send_message(message.from_user.id, "/quest - создать опрос\n"
                                           "/")


@bot.message_handler(commands=["roles"])
def view_roles(message):
    if message.from_user.username not in cfg.admins:
        return

    roles = db.get_roles()
    msg = "\n".join(
        [f"""{role.name}: {', '.join(
            [db.get_user(tg_user_id=user_id).user_str for user_id in role.get_users()]
        )} """ for role in roles]
    )
    if not msg:
        msg = "Нет ролей"
    bot.send_message(message.from_user.id, msg)


@bot.message_handler(commands=["users"])
def view_users(message):
    if message.from_user.username not in cfg.admins:
        return

    users = db.get_users()

    msg = "\n".join(
        [f"""{user.user_str}: {', '.join(user.get_roles())}"""
         for user in users]
    )
    if not msg:
        msg = "Нет пользователей"
    bot.send_message(message.from_user.id, msg)


@bot.message_handler(commands=["mkrole"])
def mkrole(message):
    if message.from_user.username not in cfg.admins:
        return

    _, user, role = parse(message.text, 3)
    if not (user and role):
        bot.send_message(message.from_user.id, "Ошибка форматирования")
        return

    role = role.strip()

    try:
        db.mkrole(user.replace("@", ""), role)
    except exc.NoResultFound:
        bot.send_message(message.from_user.id, "В системе нет такого пользователя")
    else:
        bot.send_message(message.from_user.id, "Роль установлена")


@bot.message_handler(commands=["rmrole"])
def rmrole(message):
    if message.from_user.username not in cfg.admins:
        return

    _, user, role = parse(message.text, 3)
    if not (user and role):
        bot.send_message(message.from_user.id, "Ошибка форматирования")
        return

    role = role.strip()

    try:
        db.rmrole(user.replace("@", ""), role)
    except exc.NoResultFound:
        bot.send_message(message.from_user.id, "В системе нет такого пользователя")
    else:
        bot.send_message(message.from_user.id, "Роль снята")

@bot.message_handler(commands=["quest"])
def quest(message, back=False):
    if message.from_user.username not in cfg.admins:
        return

    question = database_handler.Question()

    bot.send_message(message.from_user.id, "Текст опроса:", reply_markup=get_quest_keyboard())
    bot.register_next_step_handler(message, quest2, question)


def quest2(message, question, back=False):
    if not back:
        if message.text == "Отмена":
            return

        question.text = message.text
    bot.send_message(message.from_user.id, "Варианты ответа (через точку с запятой):",
                     reply_markup=get_quest2_keyboard())
    bot.register_next_step_handler(message, quest3, question)


def quest3(message, question, back=False):
    if not back:
        if message.text == "Назад":
            quest(message, question, True)
            return
        if message.text == "Отмена":
            return

        if message.text == "Опрос с развернутым ответом":
            question.answer_options_json = json.dumps([])
        else:
            options = message.text.split(';')
            for i, option in enumerate(options):
                options[i] = option.strip(" ").capitalize()
            question.answer_options_json = json.dumps(options)

    bot.send_message(message.from_user.id,
                     "Роли и пользователи (@имя), для которых предназначен опрос (через точку с запятой):",
                     reply_markup=get_quest3_keyboard())
    bot.register_next_step_handler(message, quest4, question)


def quest4(message, question, back=False):
    if not back:
        if message.text == "Назад":
            quest2(message, question, True)
            return
        if message.text == "Отмена":
            return

        if message.text == "Опрос для всех":
            question.for_all = True
        else:
            question.for_all = False
            groups = message.text.split(";")

            roles = []
            users = []

            for group in groups:
                if "@" in group:
                    username = group[group.find("@") + 1:].strip()
                    try:
                        users.append(db.get_user(username=username).tg_user_id)
                    except exc.NoResultFound:
                        bot.send_message(message.from_user.id, f"@{username} нет в системе")
                else:
                    roles.append(group.strip())

            question.users_for_json = json.dumps(users)
            question.roles_for_json = json.dumps(roles)

        bot.send_message(message.from_user.id,
                         "Это обязательный вопрос?",
                         reply_markup=get_quest4_keyboard())
        bot.register_next_step_handler(message, quest5, question)


def quest5(message, question, back=False):
    if not back:
        if message.text == "Назад":
            quest3(message, question, True)
            return
        if message.text == "Отмена":
            return

        if message.text == "Да":
            question.optional = False
        elif message.text == "Нет":
            question.optional = True
        else:
            bot.send_message(message.from_user.id, "Напишите, да или нет")
            quest4(message, question, True)
            return

    bot.send_message(message.from_user.id,
                     "Введите дату и время отправки опроса в формате ДД.ММ.ГГГГ ЧЧ:ММ",
                     reply_markup=get_quest5_keyboard())
    bot.register_next_step_handler(message, quest6, question)


def quest6(message, question, back=False):
    if not back:
        if message.text == "Назад":
            quest3(message, question, True)
            return
        if message.text == "Отмена":
            return

        if message.text == "Отправить прямо сейчас":
            question.send_datetime = datetime.datetime.now()
        else:
            try:
                date, time = message.text.split(" ")
                day, month, year = map(int, date.split("."))
                hour, minute = map(int, time.split(":"))

                question.send_datetime = datetime.datetime(year, month, day, hour, minute, 0)
            except Exception:
                bot.send_message(message.from_user.id,
                                 "Ошибка форматирования, повторите",
                                 reply_markup=get_quest5_keyboard())
                quest5(message, question, True)
                return

        db.create_question(question)
        bot.send_message(message.from_user.id,
                         "Опрос добавлен!",
                         reply_markup=RemoveMarkup())


def parse(text, n):
    for i in range(n - 1):
        part, text = text[:text.find(" ")], text[text.find(" ") + 1:]
        yield part
    yield text


def msg_user_to_username(user):
    return f"{user.first_name} {(user.last_name if user.last_name else '')} " \
           f"{(('(@' + user.username + ')') if user.username else '')}"


"""Keyboards:"""


def get_help_keyboard():
    keyboard = types.ReplyKeyboardMarkup()
    key_join = types.KeyboardButton('/join')
    key_status = types.KeyboardButton('/status')
    keyboard.row(key_join, key_status)
    return keyboard


def get_quest_keyboard():
    keyboard = types.ReplyKeyboardMarkup()
    key_cancel = types.KeyboardButton('Отмена')
    keyboard.row(key_cancel)
    return keyboard


def get_quest2_keyboard():
    keyboard = types.ReplyKeyboardMarkup()
    key_no_options = types.KeyboardButton('Опрос с развернутым ответом')
    keyboard.row(key_no_options)
    key_back = types.KeyboardButton('Назад')
    key_cancel = types.KeyboardButton('Отмена')
    keyboard.row(key_back, key_cancel)
    return keyboard


def get_quest3_keyboard():
    keyboard = types.ReplyKeyboardMarkup()
    key_no_options = types.KeyboardButton('Опрос для всех')
    keyboard.row(key_no_options)
    key_back = types.KeyboardButton('Назад')
    key_cancel = types.KeyboardButton('Отмена')
    keyboard.row(key_back, key_cancel)
    return keyboard


def get_quest4_keyboard():
    keyboard = types.ReplyKeyboardMarkup()
    key_yes = types.KeyboardButton('Да')
    key_no = types.KeyboardButton("Нет")
    keyboard.row(key_yes, key_no)
    key_back = types.KeyboardButton('Назад')
    key_cancel = types.KeyboardButton('Отмена')
    keyboard.row(key_back, key_cancel)
    return keyboard


def get_quest5_keyboard():
    keyboard = types.ReplyKeyboardMarkup()
    key_now = types.KeyboardButton('Отправить прямо сейчас')
    keyboard.row(key_now)
    key_back = types.KeyboardButton('Назад')
    key_cancel = types.KeyboardButton('Отмена')
    keyboard.row(key_back, key_cancel)
    return keyboard


def get_admin_keyboard():
    keyboard = types.ReplyKeyboardMarkup()
    key_join = types.KeyboardButton('/join')
    key_status = types.KeyboardButton('/status')
    keyboard.row(key_join, key_status)
    return keyboard

def main():
    pass

async def polling_coro():
    while True:
        try:
            bot.polling()
        except Exception:
            print("renewing connection")

if __name__ == '__main__':
    print("running")
    bot.polling()

