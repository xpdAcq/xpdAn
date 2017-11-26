$ACTIVITIES = ['version_bump',
               'changelog',
               'tag',
               'ghrelease']

$VERSION_BUMP_PATTERNS = [
    ('xpdan/__init__.py', '__version__\s*=.*', "__version__ = '$VERSION'"),
    ('setup.py', 'version\s*=.*,', "version='$VERSION',")
    ]
$CHANGELOG_FILENAME = 'CHANGELOG.rst'
$CHANGELOG_IGNORE = ['TEMPLATE']

$GITHUB_ORG = 'xpdAcq'
$GITHUB_REPO = 'xpdAn'
$TAG_REMOTE = 'git@github.com:xpdAcq/xpdAn.git'
