from setuptools import setup, find_packages

names = [
    "save_server",
    "db_server",
    "portable_db_server",
    "viz_server",
    "analysis_server",
]

entry_points = {
    "console_scripts": [
        f"{name}= xpdan.startup.{name}:run_main" for name in names
    ]
}

setup(
    name="xpdan",
    version='0.5.0',
    packages=find_packages(),
    description="data processing module",
    zip_safe=False,
    package_data={"xpdan": ["config/*"]},
    include_package_data=True,
    url="http:/github.com/xpdAcq/xpdAn",
    entry_points=entry_points,
)
