# Top OpenSubtitles Sentences

This repository provides customizable Python code for extracting frequency lists of the most common sentences (and words) in the [OpenSubtitles2018](https://opus.nlpl.eu/OpenSubtitles-v2018.php) corpus. The code should run for all the 62 languages available.


## Usage

First download this repository and install the dependencies listed in [pyproject.toml](pyproject.toml) (Python 3.8+ and a few Python packages). If you're using [Poetry](https://python-poetry.org) this can be done automatically by running `poetry install` from the command line when at the root directory of the downloaded repository.

Next adjust the *Settings* section of [src/top_open_subtitles_sentences.py](src/top_open_subtitles_sentences.py) to your liking, while optionally reading the *Info* section, before running the whole file. 


## Features

Up to 62 languages are supported (see the *Info* section of [src/top_open_subtitles_sentences.py](src/top_open_subtitles_sentences.py) for the full list).

The Python code will by default first download the source data, parse it into a temporary file, and then construct the lists of most common sentences and (optionally) words from it. The lists also include the number of times each sentence or word occurs in the corpus.

The following cleaning steps are performed (uncomment the respective lines in the code to skip them):

- Whitespace and dashes are stripped from the beginning and end of each sentence.
- Entries consisting of only punctuation and numbers are removed.
- Sentences ending the same way up to " .?!¿¡" are combined into the most common form.
- Words of differing cases are combined into the most common case.
- Sentences in [src/extra_settings/extra_sentences_to_exclude.csv](src/extra_settings/extra_sentences_to_exclude.csv) are excluded.

The [`word_tokenize`](https://www.nltk.org/api/nltk.tokenize.html) function of the Python Natural Language Toolkit ([NLTK](https://www.nltk.org/index.html)) is used to split sentences into words.


## The underlying corpus

This repository is based on the OpenSubtitles2018 corpus, which is part of the OPUS collection. In particular, as source files the untokenized corpus files linked in the rightmost column language IDs of the first table [here](https://opus.nlpl.eu/OpenSubtitles-v2018.php) are taken. These contain the raw corpus text as a collection of xml subtitle files, which have been downloaded from [www.opensubtitles.org](https://www.opensubtitles.org) (see also their newer site [www.opensubtitles.com](https://www.opensubtitles.com)). 

[OpenSubtitles](https://www.opensubtitles.org) is an online community where anyone can upload and download subtitles. At the time of writing 6,404,981 subtitles are available, of these 3,735,070 are included in the [OpenSubtitles2018 corpus](https://opus.nlpl.eu/OpenSubtitles-v2018.php), which contains a total of 22.10 billion tokens. See the following article for a detailed description of the corpus:
P. Lison and J. Tiedemann, 2016, [OpenSubtitles2016: Extracting Large Parallel Corpora from Movie and TV Subtitles](http://www.lrec-conf.org/proceedings/lrec2016/pdf/947_Paper.pdf). In Proceedings of the 10th International Conference on Language Resources and Evaluation (LREC 2016).


## Related work

There exist at least two similar but older repositories (older versions of the corpus and older Python versions) which can parse the OpenSubtitles corpus but do not extract lists of the most frequent sentences and words: [AlJohri/OpenSubtitles](https://github.com/AlJohri/OpenSubtitles) and [domerin0/opensubtitles-parser](https://github.com/domerin0/opensubtitles-parser).

The [orgtre/google-books-ngram-frequency](https://github.com/orgtre/google-books-ngram-frequency) repository constructs similar lists of the most frequent words and sequences of up to five words (ngrams) in the much larger Google Books Ngram Corpus.


## Limitations and known problems

If a movie or episode contains several different subtitle files for a given langague in the raw corpus, then all of them are used when constructing the sentence/word lists. Since more popular movies and episodes tend to have more subtitle files, the resulting lists can hence be viewed as popularity-weighted.

Almost no language-specific corrections have been made and not all languages have been tested. In particular, adaptations might have to be made for languages not using a Latin script.

The sentence and word lists still contain entries which probably better are excluded for many purposes, like proper names.

The code in this repository is licensed under the [Creative Commons Attribution 3.0 Unported License](https://creativecommons.org/licenses/by/3.0/). The sentence and word lists come with the same license as [the underlying corpus](http://opus.nlpl.eu/OpenSubtitles-v2018.php). Issue reports and pull requests are most welcome!
