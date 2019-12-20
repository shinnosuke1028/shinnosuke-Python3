# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/9/4 0:10
# @File: DuoLaAMeng.py


class DuoLa(object):
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def getzhuqingting(self):
        print('拿出一个竹蜻蜓给' + self.name)
        print(self.age)


class Student(object):
    def __init__(self, name, score):
        self.name = name
        self.score = score

    def print_score(self):
        print('%s: %s' % (self.name, self.score))

    def get_grade(self):
        if self.score >= 90:
            return 'A'
        elif self.score >= 60:
            return 'B'
        else:
            return 'C'


# Class 私有变量保护
class User(object):
    def __init__(self, name=None, passwd=None):
        self.__name = name
        self.__passwd = passwd

    def get_name(self):
        return self.__name

    def set_name(self, name):
        if 4 <= len(name) <= 8:
            self.__name = name
        else:
            print('用户名长度无效<4~8>，未成功设置.')

    # 属性捆绑，将方法转换为变量的感觉
    name = property(fget=get_name, fset=set_name)

    def get_passwd(self):
        return self.__passwd

    def set_passwd(self, passwd):
        if 6 <= len(passwd) <= 8:
            self.__name = passwd
        else:
            print('用户密码长度无效<6~8>，未成功设置.')


class Base():
    def __init__(self, character='%'):
        self.character = character

    def show(self):
        return self.character


class Derived(Base):
    def __init__(self, age):
        super().__init__()
        self.derived_data = 99
        self.age = age

    def derived_Show(self):
        print(str(self.derived_data), '\t', str(self.age) + super().show())


class A():
    def __init__(self):
        self.X = Derived(14)


if __name__ == '__main__':
    # duola = DuoLa(name='大雄', age=12)
    # duola.getzhuqingting()

    # bart = Student('Bart Simpson', 59)
    # lisa = Student('Lisa Simpson', 87)
    # print('bart.name =' + bart.name)
    # print('bart.score =', bart.score)
    # bart.print_score()
    # print('grade of Bart:', bart.get_grade())
    # print('grade of Lisa:', lisa.get_grade())

    # bart.age = 9
    # print(bart.score)
    # print(bart.age + bart.score)

    # 测试:
    # bart = Student_2('Bart', 'male')
    # if bart.get_gender() != 'male':
    #     print('测试失败!')
    # else:
    #     bart.set_gender('female')
    #     if bart.get_gender() != 'female':
    #         print('测试失败!')
    #     else:
    #         print('测试成功!')

    # a = A()
    # a.X.derived_Show()

    b = Derived(age=14)
    b.character = '!'
    b.derived_Show()
