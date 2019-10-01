from distutils.core import setup
from simex.version import version
import sys

if sys.version_info < (3, 5):
      sys.exit("Unsupported python version")

setup(name="simex", version=".".join(str(version)), description="RxSimex remote interface", author="Yuanyi Wu",
      url="https://github.com/wuyuanyi135/RxSimex", packages=["simex"], install_requires=["rx", "msgpack"])
