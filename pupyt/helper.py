""" VARS """


def starts_with(string):
    def _starts_with(value):
        return string == value[:len(string)]
    return _starts_with


def ends_with(string):
    def _ends_with(value):
        return string == value[-len(string):]
    return _ends_with


def is_in(my_list):
    def _is_in(value):
        return value in my_list
    return _is_in

def all_but(my_list):
    def _all_but(value):
        return value not in my_list
    return _all_but

