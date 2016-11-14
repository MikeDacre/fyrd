This folder contains sphinx documentation sources to build documentation for
the whole project.  The documentation already exists at
`<https://fyrd.readthedocs.org>`_ and in the `fyrd.pdf <../fyrd.pdf>`_ document
in the root of this repository.

To build it again, just run `make html latexpdf` in this directory, the pdf will
appear here, and the html will appear in _build/html.

You can also create documentation in any format of your choice, just run `make`
in this directory to get a list of possible formats.

The sources for the documentation can be found in the sphinx directory. Note that
_build is excluded from the repository, and so no files built here will appear in
the repo.

In addition the `local_queue.ipnb <local_queue.ipnb>`_ file contains a
proof-of-principle for the local queue with dependency tracking idea.
