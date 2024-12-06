#!/bin/bash
mkdir -p aux_files
pdflatex -output-directory=aux_files report.tex
mv aux_files/report.pdf .