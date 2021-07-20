# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
import sphinx_rtd_theme

sys.path.insert(0, os.path.abspath('../../'))


# -- Project information -----------------------------------------------------

project = 'cms-htcondor-es'
project = 'cms-htcondor-es'
copyright = '2021, Brian P Bockelman(bbockelm), Benjamin Stieger(stiegerb), Christian Ariza(cronosnull), Valentin Kuznetsov(vkuznet), Ceyhun Uzunoglu(mrceyhun), todor-ivanov, Kenyi Hurtado(khurtado), Leonardo Cristella(lecriste), Stefano Belforte(belforte)'
author = 'Brian P Bockelman(bbockelm), Benjamin Stieger(stiegerb), Christian Ariza(cronosnull), Valentin Kuznetsov(vkuznet), Ceyhun Uzunoglu(mrceyhun), todor-ivanov, Kenyi Hurtado(khurtado), Leonardo Cristella(lecriste), Stefano Belforte(belforte)'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ['myst_parser', 'sphinx.ext.todo', 'sphinx.ext.viewcode', 'sphinx.ext.autodoc', 'sphinx_rtd_theme']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]
