#!/bin/sh

cd test
coverage2 run -m unittest discover
coverage2 report
coverage2 html -d ../coverage
