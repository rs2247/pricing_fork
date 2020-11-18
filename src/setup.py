import setuptools


setuptools.setup(
    name="quero-pricing",
    version="0.0.1",
    author="Quero Pricing",
    author_email="pricing@quero.com",
    description="Pricing Package",
    long_description="Pricing Package",
    long_description_content_type="text/markdown",
    url="https://gitlab.com/queroeducacao-ds/pricing",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    package_data={ 'pricing.operations.alternative_formulas': ['*.sql'], 'pricing.operations.experiments': ['*.sql'] },
    include_package_data=True,
    # py_modules=["fibs"],
    python_requires='>=3.6',
)
