# -*- coding: utf-8 -*-
"""
Project metadata.

Information describing the project.
"""
try:
    from importlib.metadata import metadata
except ImportError:
    from importlib_metadata import metadata  # type: ignore


# The package name, which is also the "UNIX name" for the project.
package = "yeelight"
project = "python-yeelight"
project_no_spaces = project.replace(" ", "")
_package_metadata = metadata(package)
version = _package_metadata["Version"]
description = _package_metadata["Summary"]
authors = ["Stavros Korokithakis"]
authors_string = ", ".join(authors)
emails = ["hi@stavros.io"]
license = "BSD"
copyright = "2016 " + authors_string
url = "https://gitlab.com/stavros/python-yeelight"
