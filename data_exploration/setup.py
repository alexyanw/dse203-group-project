from setuptools import setup, find_packages
setup(name="ProductRecommendations_EDA",
      version="0.1",
      packages=['dora'],
      install_requires=[
            'psycopg2',
            'pysolr',
            'sklearn',
            'pandas'
      ],
      zip_safe=False)