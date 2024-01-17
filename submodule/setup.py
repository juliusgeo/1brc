from setuptools import setup, Extension

module1 = Extension('brc',
                    sources = ['main.c'])

setup (name = 'brc',
       version = '1.0',
       description = 'This is a demo package',
       ext_modules = [module1])