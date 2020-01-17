# Goobi viewer - Indexer
[![](https://github.com/intranda/goobi-viewer-indexer/workflows/.github/workflows/default.yml/badge.svg)
> Indexing application as part of the highly flexible digital library framework - made by the Goobi developer team


## Indexer for the digital library framework
The Goobi viewer indexer is responsible for indexing metadata files and related content files for the Goobi viewer. It parses provided metadata files (e.g. METS and LIDO) to fill the Solr search index with all relevant information. Additionally all fulltext files are read and indexed to allow a fulltext search in the Goobi viewer.


## Community
You can get in touch with the communiy in the forum. Currently the most is happening in German but please feel free to ask any questions there in English too:

https://community.goobi.io

You can find a list of Goobi viewer installations at the following URL:

https://goobi.io/viewer/installations


## Documentation
The documentation for the Goobi viewer can be found using the following URLs:

* [German](https://docs.intranda.com/goobi-viewer-de/)
* [English](https://docs.intranda.com/goobi-viewer-en/)


## Development
The development of the Goobi viewer in mostly happening by the software company [intranda GmbH](https://intranda.com). All current developments are centrally listed and explained inside of the monthy digests:

* [German](https://docs.intranda.com/goobi-viewer-digests-de/)
* [English](https://docs.intranda.com/goobi-viewer-digests-en/)


## Technical background
The Goobi viewer consists of multiple packages which all have to be installed and configured properly:

| Package                                                                                  | Function                                                                     |
| ------                                                                                   | ------                                                                       |
| [Goobi viewer Core](https://github.com/intranda/goobi-viewer-core)                       | Core functionality of the viewer application                                 |
| [Goobi viewer Indexer](https://github.com/intranda/goobi-viewer-indexer)                 | Indexing application to fill the Solr search index with metadata information |
| [Goobi viewer Connector](https://github.com/intranda/goobi-viewer-connector)             | Connectors for different use cases (incl. OAI-PMH, SRU)                      |
| [Goobi viewer Theme Reference](https://github.com/intranda/goobi-viewer-theme-reference) | Reference Theme for the styling of the web pages for the user interface      |


## Licence
The Goobi viewer is released under the license GPL2 or later.
Please see ``LICENSE`` for more information.

