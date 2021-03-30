import sys
from probe import ConfigTester

def square(x):
    return x*x

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print('Need the path of a config file as a command line argument')
        sys.exit(-1)

    config_tester = ConfigTester(sys.argv[1])
    config_tester.check_channel()
    config_tester.check_provider(disable_launcher=True)
    config_tester.check_provider(disable_launcher=False)
    config_tester.check_executor(square, 3, 9)
