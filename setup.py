import setuptools

setuptools.setup(
    name='acdc_train',
    version='1.0.0',
    author='David A. Smith',
    author_email='dasmiq@gmail.com',
    description='Bootstrapping OCR training data',
    url='https://github.com/OpenITI/acdc_train/',
    packages=setuptools.find_packages(include=['acdc_train']),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
    ],
    install_requires=[
        'pyspark>=3.0.1'
     ],
    scripts=['bin/acdc-run', 'bin/pdf_images.py', 'bin/batkraken.sh'],
    data_files=[('share', ['share/alto-lines.py', 'share/openiti-files.py',
                           'share/patch-alto.py', 'share/wit-lines.py'])],
    python_requires='>=3.6',
)
