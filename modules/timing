import time


def calculate_time(func):
    def inner1(*args, **kwargs):
        begin = time.time()
        func(*args, **kwargs)
        end = time.time()
        duration = end - begin
        print(f'{func.__name__} had a duration of: {duration}')

    return inner1


def time_all_class_methods(cls):
    class NewClass:
        def __init__(self, *args, **kwargs):
            self.object_instance = cls(*args, **kwargs)

        def __getattribute__(self, name):
            try:
                x = super(NewClass, self).__getattribute__(name)
            except AttributeError as att:
                pass
            else:
                return x
            x = self.object_instance.__getattribute__(name)
            if type(x) == type(self.__init__):  # it is an instance method
                return calculate_time(x)
            else:
                return x

    return NewClass
