from setuptools import setup


def parse_requirements(path):
  requires = []
  with open(path) as requirements:
    for line in requirements:
      # Append everything before the first comment character.
      requires.append(line.partition("#")[0].rstrip())
  return requires


setup(name="curie",
      author="Nutanix, Inc.",
      use_scm_version=True,
      setup_requires=['setuptools_scm'],
      description="Component of X-Ray that executes scenarios",
      author_email="xray@nutanix.com",
      license="All Rights Reserved",
      packages=["curie",
                "curie.iogen",
                "curie.net",
                "curie",
                "curie.powershell",
                "curie.result",
                "curie.steps",
                "curie.testing"
               ],
      scripts=["bin/curie_cmd_wrapper",
               "bin/curie_unix_agent"],
      install_requires=parse_requirements("requirements.txt"),
      tests_require=parse_requirements("requirements-dev.txt"),
      test_suite="tests.run_unit.suite",
      include_package_data=True,
      zip_safe=True)
