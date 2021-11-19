import datetime
import copy
import json

import sqlalchemy
from sqlalchemy import Column, Integer, String, DateTime, Boolean, desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import bot_config as cfg

Base = declarative_base()


class Role(Base):
    __tablename__ = "roles"
    name = Column(String, primary_key=True)
    users_json = Column(String)  # json-dumped list

    def __init__(self, name):
        self.name = name
        self.users_json = json.dumps([])

    def add_user(self, tg_user_id):
        users = json.loads(self.users_json)
        users.append(tg_user_id)
        self.users_json = json.dumps(list(set(users)))

    def remove_user(self, tg_user_id):
        users = json.loads(self.users_json)
        users.pop(users.index(tg_user_id))
        self.users_json = json.dumps(list(set(users)))

    def get_users(self):
        return json.loads(self.users_json)


class User(Base):
    __tablename__ = "users"
    tg_user_id = Column(Integer, primary_key=True)
    username = Column(String)
    user_str = Column(String)
    roles_json = Column(String)
    admin = Column(Boolean)
    answered_last_question = Column(Boolean)
    last_question_notifications = Column(Integer)
    bx_id = Column(Integer)

    def __init__(self, tg_user_id, username=None, user_str=None):
        self.tg_user_id = tg_user_id
        self.username = username
        self.user_str = user_str
        self.roles_json = json.dumps([])
        self.admin = username in cfg.admins
        self.answered_last_question = True
        self.last_question_notifications = 0

    def add_role(self, role):
        roles = json.loads(self.roles_json)
        roles.append(role)
        self.roles_json = json.dumps(list(set(roles)))

    def remove_role(self, role):
        roles = json.loads(self.roles_json)
        try:
            roles.pop(roles.index(role))
        except ValueError:
            pass
        else:
            self.roles_json = json.dumps(roles)

    def get_roles(self):
        return json.loads(self.roles_json)


class Question(Base):
    __tablename__ = "questions"
    id = Column(Integer, primary_key=True)
    text = Column(String)
    for_all = Column(Boolean)
    roles_for_json = Column(String)
    users_for_json = Column(String)
    answer_options_json = Column(String)
    optional = Column(Boolean)
    send_datetime = Column(DateTime)
    sent = Column(Boolean)
    sent_to_json = Column(String)

    def __init__(self, text: str = '', for_all: bool = False, roles_for: list = [], users_for: list = [],
                 answer_options: list = [], optional: bool = [],
                 send_datetime: datetime.datetime = None):
        self.text = text
        self.for_all = for_all
        self.roles_for_json = json.dumps(roles_for)
        self.users_for_json = json.dumps(users_for)
        self.answer_options_json = json.dumps(answer_options)
        self.optional = optional
        self.send_datetime = send_datetime
        self.sent = False
        self.sent_to_json = json.dumps([])

    def get_roles_for(self):
        return json.loads(self.roles_for_json)

    def get_users_for(self):
        return json.loads(self.users_for_json)

    def get_answer_options(self):
        return json.loads(self.answer_options_json)

    def get_sent_to(self):
        return json.loads(self.sent_to_json)


class Answer(Base):
    __tablename__ = "answers"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    question_id = Column(Integer)
    text = Column(String)

    def __init__(self, user_id, question_id, text):
        self.user_id = user_id
        self.question_id = question_id
        self.text = text


class Handler:
    database_path = "database.db"

    def __init__(self, database_path=None, base=Base):
        if database_path:
            self.database_path = database_path
        engine = sqlalchemy.create_engine(f"sqlite:///{self.database_path}" + '?check_same_thread=False')
        base.metadata.create_all(engine)
        self.session = sessionmaker(bind=engine, expire_on_commit=False)

    def create_role(self, name):
        if not name in [role.name for role in self.get_roles()]:
            session = self.session()
            role = Role(name)
            session.add(role)
            session.commit()

    def remove_role(self, name):
        session = self.session()
        role = session.query(Role).filter(Role.name == name).one()
        users = role.get_users()
        for user in users:
            user = session.query(User).filter(User.tg_user_id == user).one()
            user.remove_role(name)
        session.delete(role)
        session.commit()

    def get_role(self, name):
        session = self.session()
        role = copy.deepcopy(session.query(Role).filter(Role.name == name).one())
        session.close()
        return role

    def create_user(self, tg_user_id, username=None, user_str=None):
        session = self.session()
        if session.query(User).filter(User.tg_user_id == tg_user_id).first():
            return
        user = User(tg_user_id, username, user_str)
        session.add(user)
        session.commit()

    def remove_user(self, tg_user_id):
        session = self.session()
        user = session.query(User).filter(User.tg_user_id == tg_user_id).one()
        roles = user.get_roles()
        for role in roles:
            self.get_role(role).remove_user(tg_user_id)
        session.delete(user)
        session.commit()

    def get_user(self, tg_user_id=None, username=None):
        session = self.session()
        if tg_user_id:
            user = copy.deepcopy(session.query(User).filter(User.tg_user_id == tg_user_id).one())
        elif username:
            user = copy.deepcopy(session.query(User).filter(User.username == username).one())
        else:
            raise AttributeError("tg_user_id or username were not given")
        session.close()
        return user

    def get_roles(self):
        session = self.session()
        roles = copy.deepcopy(session.query(Role).all())
        session.close()
        return roles

    def get_users(self):
        session = self.session()
        users = copy.deepcopy(session.query(User).all())
        session.close()
        return users

    def mkrole(self, username, role):
        session = self.session()
        user = session.query(User).filter(User.username == username).one()
        user.add_role(role)
        self.create_role(role)
        role = session.query(Role).filter(Role.name == role).one()
        role.add_user(user.tg_user_id)
        session.commit()

    def rmrole(self, username, role):
        session = self.session()
        user = session.query(User).filter(User.username == username).one()
        user.remove_role(role)
        role = session.query(Role).filter(Role.name == role).one()
        role.remove_user(user.tg_user_id)
        session.commit()

    def create_question(self, question_obj):
        session = self.session()
        session.add(question_obj)
        session.commit()

    def get_outdated_question(self):
        session = self.session()
        question = session.query(Question).filter(Question.sent == False). \
            filter(Question.send_datetime <= datetime.datetime.now()).first()
        if not question:
            return None
        session.close()
        return copy.deepcopy(question)

    def create_answer(self, tg_user_id, question_id, text):
        answer = Answer(tg_user_id, question_id, text)
        session = self.session()
        session.add(answer)
        session.commit()

    def get_questions(self):
        session = self.session()
        questions = copy.deepcopy(session.query(Question).order_by(desc(Question.send_datetime)).all())
        session.close()
        return questions

    def get_question(self, question_id):
        session = self.session()
        question = copy.deepcopy(session.query(Question).filter(Question.id == question_id).one())
        session.close()
        return question

    def get_answers(self, question_id=None, tg_user_id=None, role=None):
        session = self.session()
        if question_id:
            if not role:
                answers = copy.deepcopy(session.query(Answer).filter(Answer.question_id == question_id).all())
            else:
                answers = copy.deepcopy(session.query(Answer).filter(Answer.question_id == question_id).all())
                tmp = []
                for answer in answers:
                    if role in self.get_user(answer.user_id).get_roles():
                        tmp.append(answer)
                answers = tmp
        elif tg_user_id:
            answers = copy.deepcopy(session.query(Answer).filter(Answer.user_id == tg_user_id).all())
        else:
            raise AttributeError("Attrs were not given")
        session.close()
        return answers

    def update_user(self, tg_id, answered_last_question=None, last_question_notifications=None, bx_id=None):
        session = self.session()
        user = session.query(User).filter(User.tg_user_id == tg_id).one()
        if not answered_last_question is None:
            user.answered_last_question = answered_last_question
        if not last_question_notifications is None:
            user.last_question_notifications = last_question_notifications
        if not bx_id == None:
            user.bx_id = bx_id
        session.commit()

    def update_question(self, id_, sent_to=None, sent=None):
        session = self.session()
        question = session.query(Question).filter(Question.id == id_).one()
        if not sent_to is None:
            question.sent_to_json = json.dumps(sent_to)
        if not sent is None:
            question.sent = sent

        session.commit()
