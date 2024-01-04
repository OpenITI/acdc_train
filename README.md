# Automatic Collation for Diversifying Corpora (ACDC)

This package provides code for producing training data for optical character recognition and handwritten text recogntion (OCR and HTR) by aligning the output of an initial model on a collections of images with a collection of digital editions of similar texts.

For background and a walkthrough of using these tools, see [the video tutorial](https://www.youtube.com/watch?v=kNx4GyH5HSo).

First, install [`passim`](https://github.com/dasmiq/passim). Then install [`kraken`](https://github.com/mittagessen/kraken).  If you want to start with PDF files of books rather than page images, use the `pdf` option:
```
pip install --user kraken[pdf]
```

After this is complete, the programs `seriatim`, `kraken`, and `ketos` should be in your PATH and available on the command line.

Install the scripts in this package with:
```
pip install --user .
```

We use `make` to manage OCR of a potentially large number of input pages.  Create a directory for your work, go into that directory, and link to the `Makefile` in this package:
```
ln -s <path to src>/acdc_train/etc/Makefile
```

If you're starting with PDF files, put them in a subdirectory named `pdf`.  If you're starting with individual page image files instead, create a directory named `images` with subdirectories each containing the page image files for a book.

If you put plain text files in a directory named `electronic_texts`, they will be interpreted with OpenITI markdown.  If you prefer, you could put JSONL-formatted input in `electronic_texts.json`.  This uses the `passim` conventions of an `id` field for a unique document identifier and `text` field, potentially with escaped newlines, for the text.

In the paper, we bootstrapped training starting from [page segmentation and transcription models](https://github.com/OpenITI/arabic_script_ocr_models) trained on printed Arabic-script books.  You can change the `segment` and `ocr` variables in the `Makefile` to train from a different starting model.

You should then be able to run experiments with three rounds of OCR'ing the pages in `pdf` or `images` and retraining by running this `make` command:
```
make all
```

If you have a GPU that works with kraken, uncomment the line near the top of the Makefile to use that device with kraken:
```
KRAKEN_DEVICE=-d cuda:0
```

If any of the steps in the pipeline complain about running out of memory, edit the line near to the top of the Makefile to give spark more than 4GB of memory:
```
export SPARK_SUBMIT_ARGS=--executor-memory 4G --driver-memory 4G
```
