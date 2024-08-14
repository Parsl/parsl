from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parsl.channels.base import Channel
    from parsl.channels.local.local import LocalChannel

lazys = {
        'Channel': 'parsl.channels.base',
        'LocalChannel': 'parsl.channels.local.local',
}

import parsl.channels as px


def lazy_loader(name):
    if name in lazys:
        import importlib
        m = lazys[name]
        # print(f"lazy load {name} from module {m}")
        v = importlib.import_module(m)
        # print(f"imported module: {v}")
        a = v.__getattribute__(name)
        px.__setattr__(name, a)
        return a
    raise AttributeError(f"No (lazy loadable) attribute in {__name__} for {name}")


px.__getattr__ = lazy_loader  # type: ignore[method-assign]

__all__ = ['Channel', 'LocalChannel']
