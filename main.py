# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    _capital = 100000
    _buy_capital = _capital // 3
    print(_buy_capital)
    _c = True
    while _c:
        _capital = _capital - _buy_capital
        if _capital < 0:
            _c = False
        else:
            print()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
