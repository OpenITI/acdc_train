# Automatic Collation for Diversifying Corpora (ACDC)

This package provides code for producing training data for optical character recognition and handwritten text recogntion (OCR and HTR) by aligning the output of an initial model on a collections of images with a collection of digital editions of similar texts.

First, install `passim` and `kraken`.

Install the scripts in this package with:
```
pip install --user .
```

We use `make` to manage OCR of a potentially large number of input pages.  Create a directory for your work, go into that directory, and link to the `Makefile` in this package:
```
ln -s <path to src>/acdc_train/etc/Makefile
```

If you're starting with PDF files, put them in a subdirectory named `pdf`.  If you're starting with individual page image files instead, create a directory named `images` with subdirectories each containing the page image files for a book.

You should then be able to run experiments with three rounds of OCR'ing the books in `pdf` or `images` and retraining by running this `make` command:
```
make gen2-print-n7m5.out/alto-union/ft_best.mlmodel
```
