import parsl


@parsl.bash_app
def test_bulk_bashApps():
    return 'echo "Hello World!"'


apps = []


def test_radical_bulk(n=100):
    for _ in range(n):
        t = test_bulk_bashApps()
        apps.append(t)
    [app.result() for app in apps]


if __name__ == "__main__":
    test_radical_bulk()
