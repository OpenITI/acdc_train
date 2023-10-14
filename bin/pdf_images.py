#!/usr/bin/env python3
"""
A script splitting PDFs and converting them like eScriptorium.
"""
import os
import click
import json
import pyvips
from collections import defaultdict
from PIL import Image
from io import BytesIO

@click.command()
@click.option('-d', '--dpi', default=300, show_default=True,
              help='Output DPI.')
@click.option('-p', '--pattern', default='%s_page_%d.png', show_default=True,
              help='Pattern for filenames of individual pages.')
@click.option('-n', '--offset', default=1, show_default=True,
              help='Offset for file numbers.')
@click.argument('file')
def cli(dpi, pattern, offset, file):
    """
    A script splitting PDFs and converting them like eScriptorium.
    """
    doc = pyvips.Image.pdfload(file, n=-1, access='sequential')
    n_pages = doc.get('n-pages')
    for page_nb in range(n_pages):
        page = pyvips.Image.pdfload(file,
                                    page=page_nb,
                                    dpi=dpi,
                                    access='sequential')
        pdfname = os.path.basename(file)
        fname = pattern.format(pdfname, page_nb + offset)
        print(fname)
        page.write_to_file(fname)


if __name__ == '__main__':
    cli()

