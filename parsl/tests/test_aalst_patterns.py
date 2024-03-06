import random
import time

import pytest

from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config

pytestmark = pytest.mark.skip('not asserting anything')


local_config = config


@python_app
def multiply_rand(x):
    return x * random.randint(1, 10)


@python_app
def square_sum(x, y):
    return (x + y) ** 2


@python_app
def double_sum(x, y):
    return 2 * (x + y)


@python_app
def subtract(x, y):
    return y - x


@python_app
def add_two(x):
    return x + 2


@python_app
def rand():
    x = random.randint(1, 100)
    return x


@python_app
def square(x):
    z = x ** 2
    return z


@python_app
def cubed(x):
    return x ** 3


@python_app
def sleep_cubed(x):
    if x < 5:
        time.sleep(7)
        return x ** 3
    else:
        return x ** 3


@python_app
def increment(x):
    return x + 1


@python_app
def slow_increment(x, dur=1):
    import time
    time.sleep(dur)
    return x + 1


@python_app
def join(inputs=[]):
    return sum(inputs)


@python_app
def join_three(x, y, z):
    return x + y + z


@python_app
def sleep_square(x):
    if x > 5:
        return x ** 2
    else:
        time.sleep(5)
        return x ** 2


@python_app
def sum_results(x=[]):
    total = 0
    for r in x:
        total += r.result()
    return total


@python_app
def add(x, y):
    return x + y


@python_app
def sum_list(x=[]):
    return sum(x)


@python_app
def sum_lists(x=[], y=[], z=[]):
    total = 0
    for i in range(len(x)):
        total += x[i].result()
    for j in range(len(y)):
        total += y[j].result()
    for k in range(len(z)):
        total += z[k].result()
    return total


@python_app
def sum_elements(x=[], y=[], z=[]):
    total = 0
    for i in range(len(x)):
        total += x[i].result()
    for j in range(len(y)):
        if y[j] is not None:
            total += y[j].result()
    for k in range(len(z)):
        total += z[k].result()
    return total


@python_app
def simple_sum_elements(x, y, z):
    return x + y + z


@python_app
def eval_number(x):
    if x > 5:
        print("Larger than 5")
        return return_one().result()
    else:
        print("Less than or equal to 5")
        return return_zero().result()


@python_app
def return_one():
    return 1


@python_app
def return_zero():
    return 0


@python_app
def xor_split():
    """Test XOR split. Do A if x else B
    """
    x = rand()

    if x.result() > 5:
        print("Result > 5")
        return 0
    else:
        print("Result < 5")
        return 1


@python_app
def arb_rand():
    x = random.randint(2, 10)
    print(x)
    print("random")
    return arb_square(x).result()


@python_app
def arb_square(x):
    y = x ** 2
    if y > 25:
        return increment(y).result()
    else:
        return arb_cubed(x).result()


@python_app
def arb_cubed(x):
    y = x ** 3
    return arb_square(y).result()


def test_increment_p1a(depth=5):
    """Test simple pipeline A->B...->N
    """
    futs = {0: 0}
    for i in range(1, depth):
        futs[i] = increment(futs[i - 1])

    print([futs[i].result() for i in futs if not isinstance(futs[i], int)])


def test_increment_slow_p1b(depth=4):
    """Test simple pipeline A->B...->N with delay
    """
    futs = {0: 0}
    for i in range(1, depth):
        futs[i] = slow_increment(futs[i - 1], 0.5)

    print(futs[i])
    print([futs[i].result() for i in futs if not isinstance(futs[i], int)])


def test_sequence_p1c(x=5):
    flights = []
    miles = []
    for i in range(x):
        flights.append(rand())
    for j in range(len(flights)):
        miles.append(multiply_rand(flights[j].result()))
    for k in range(len(miles)):
        print(miles[k].result())


def test_and_split_p2a(depth=5):
    """Test simple pipeline A->B...->N
    """
    futs = {}
    for i in range(depth):
        futs[i] = increment(i)

    print([futs[i].result() for i in futs])


def test_parallel_split_p2b(x=4):
    for i in range(x):
        num = rand().result()
        y = square(num).result()
        z = cubed(num).result()
        print(y)
        print(z)


def test_and_join_p3a(depth=5):
    """Test simple pipeline A->B...->N
    """
    futs = {}
    for i in range(depth):
        futs[i] = increment(i)

    x = join(inputs=futs.values())
    print("Final sum: ", x.result())
    assert x.result() == sum([i + 1 for i in range(depth)])


def test_synchronization_p3b(x=5):
    i = []
    for j in range(x):
        a = increment(j).result()
        b = square(j).result()
        i.append(add(a, b).result())
    total = sum(i)
    print(total)


def test_xor_split_p4a():
    """Test XOR split. Do A if x else B
    """
    x = rand()

    if x.result() > 5:
        print("Result > 5")
    else:
        print("Result < 5")


def test_xor_split_p4b(x=4):
    num = []
    for i in range(x):
        num.append(eval_number(rand().result()))
    for i in range(len(num)):
        print(num[i].result())


def test_xor_join_p5(x=2):
    print(join_three(square(x).result(), increment(
        x).result(), cubed(x).result()).result())


def test_or_split_p6(x=4, y=5):
    if x < 5:
        print(add(x, y).result())
    if y > 7:
        print(subtract(x, y).result())
    if x >= 5:
        print(square_sum(x, y).result())
    if y <= 7:
        print(double_sum(x, y).result())


def test_synchronizing_merge_p7(x=4, y=5):
    num = []
    if x < 5:
        num.append(add(x, y).result())
    if y > 7:
        num.append(subtract(x, y).result())
    if x >= 5:
        num.append(square_sum(x, y).result())
    if y <= 7:
        num.append(double_sum(x, y).result())
    print(sum_list(num).result())


def test_multi_merge_p8():
    r = rand().result()
    num1 = cubed(add_two(square(r).result()).result())
    num2 = cubed(add_two(increment(r).result()).result())
    print(add(num1.result(), num2.result()).result())


def test_discriminator_p9(x=4):
    squares = []
    cubes = []
    total = []
    for i in range(x):
        squares.append(square(rand().result()))
        cubes.append(cubed(rand().result()))
        while squares[i].done() is False and cubes[i].done() is False:
            pass
        total.append(squares[i].result() + cubes[i].result())
    for i in range(x):
        print(total[i])


def test_arbitrary_cycles_p10(x=2):
    numbers = []
    for i in range(x):
        numbers.append(arb_rand())
    for i in range(x):
        print(numbers[i].result())


def test_implicit_termination_p11(x=5):
    numbers = []
    numbers.append(slow_increment(rand().result()))
    for i in range(x):
        y = rand().result()
        numbers.append(square(increment(y).result()).result())
    print(numbers[0].result())
    return


def test_multi_instances_p12(x=5):
    numbers = []
    for i in range(x):
        numbers.append(square(rand().result()))
    print(numbers[i])
    print([numbers[i].result() for i in range(len(numbers))])


def test_multi_instances_priori_p13():
    x = increment(square(rand().result()).result()).result()
    y = increment(square(rand().result()).result()).result()
    z = increment(square(rand().result()).result()).result()
    total = simple_sum_elements(x, y, z).result()
    print(total)


def test_multi_instances_without_runtime_p15():
    numbers = []
    while sum_list(numbers).result() < 20:
        numbers.append(increment(rand().result()).result())
    print(sum_list(numbers).result())


def test_multi_instances_runtime_p14(x=5):
    numbers = []
    [numbers.append(increment(square(rand().result()).result()))
     for i in range(x)]
    total = sum_results(numbers)
    print(total.result())


def test_deferred_choice_p16():
    r = rand().result()
    s = sleep_square(r)
    i = slow_increment(r)
    print(s.done())
    while s.done() is not True and i.done() is not True:
        pass
    if s.done() is True:
        print(s.result())
    elif i.done() is True:
        print(i.result())


def test_unordered_sequence_p17():
    r = rand().result()
    s = sleep_square(r)
    i = slow_increment(r)
    print(s.done())
    while s.done() is not True and i.done() is not True:
        pass
    if i.done() is True:
        print(i.result())
        print(sleep_square(i.result()).result())
    elif s.done() is True:
        print(s.result())
        print(slow_increment(s.result()).result())


def test_milestone_p18():
    r = rand().result()
    i = increment(r)
    s = sleep_square(r)
    while s.done() is not True:
        if i.done() is True:
            print(cubed(r).result())
            return


def test_withdraw_activity_p19(x=3):
    cubes = []
    squares = []
    increments = []
    for i in range(x):
        r = rand().result()
        cubes.append(sleep_cubed(r))
        squares.append(square(r))
        if cubes[i].done() is True:
            print(True)
            squares[i] = None
        else:
            print(False)
        increments.append(increment(r))
    print(sum_elements(cubes, squares, increments).result())


def test_cancel_case_p20(x=3):
    cubes = []
    squares = []
    increments = []
    for i in range(x):
        r = rand().result()
        cubes.append(cubed(r))
        squares.append(square(r))
        increments.append(increment(r))
    r = random.randint(1, 30)
    print(r)
    if r > 20:
        del cubes[:]
    if r < 20 and r > 10:
        del squares[:]
    if r < 10:
        del increments[:]
    print(sum_lists(cubes, squares, increments).result())


def test_xor_parallel_split(width=2):
    futures = {}

    for i in range(width):
        futures[i] = xor_split()

    print([futures[key].result() for key in futures])
