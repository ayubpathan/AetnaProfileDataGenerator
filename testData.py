from random import randint

class TestData():
    @classmethod
    def generate_input_data(cls, test_cases):
        tests = list()
        for each in range(test_cases):
            test_case = ['test_table_' + str(randint(1,100000)),
                         randint(1,100),
                         randint(1,100),
                         randint(1,100),
                         randint(1,100)]
            tests.append(test_case)
        return tests