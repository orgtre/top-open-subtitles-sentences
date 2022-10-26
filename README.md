# Top OpenSubtitles Sentences

This repository provides cleaned frequency lists of the most common sentences and words for all the 62 languages in the [OpenSubtitles2018](https://opus.nlpl.eu/OpenSubtitles-v2018.php) corpus, plus customizable [Python code](#python-code) which reproduces these lists.


## Lists of the most common sentences and words

Clicking on a link in the **sentences** (or **words**) column of the table below brings up a list of the 10,000 most common sentences (or 30,000 most common words) in the **language** on the corresponding row. All lists can also be found in the [bld](bld) directory.

The numbers in the **sentences** (or **words**) column give the total number of sentences (or words), including duplicates, on which the linked lists are based. These numbers differ from the ones given for the underlying corpus at [OpenSubtitles2018](https://opus.nlpl.eu/OpenSubtitles-v2018.php), primarily because they exclude the sentences and words which are removed when [cleaning](#features). Each list also contains a **count** column, which gives the number of times that particular sentence/word occurs in the underlying corpus.

| code  | language           |                                                sentences |                                              words |
|:------|:-------------------|---------------------------------------------------------:|---------------------------------------------------:|
| af    | Afrikaans          |         [60,668](bld/top_sentences/af_top_sentences.csv) |          [369,123](bld/top_words/af_top_words.csv) |
| ar    | Arabic             |     [77,863,954](bld/top_sentences/ar_top_sentences.csv) |      [360,421,023](bld/top_words/ar_top_words.csv) |
| bg    | Bulgarian          |     [92,696,697](bld/top_sentences/bg_top_sentences.csv) |      [469,206,909](bld/top_words/bg_top_words.csv) |
| bn    | Bengali            |        [623,609](bld/top_sentences/bn_top_sentences.csv) |        [2,829,891](bld/top_words/bn_top_words.csv) |
| br    | Breton             |         [22,902](bld/top_sentences/br_top_sentences.csv) |          [117,571](bld/top_words/br_top_words.csv) |
| bs    | Bosnian            |     [33,769,306](bld/top_sentences/bs_top_sentences.csv) |      [164,287,955](bld/top_words/bs_top_words.csv) |
| ca    | Catalan            |        [604,672](bld/top_sentences/ca_top_sentences.csv) |        [3,645,619](bld/top_words/ca_top_words.csv) |
| cs    | Czech              |    [134,738,901](bld/top_sentences/cs_top_sentences.csv) |      [640,319,278](bld/top_words/cs_top_words.csv) |
| da    | Danish             |     [29,963,011](bld/top_sentences/da_top_sentences.csv) |      [158,465,190](bld/top_words/da_top_words.csv) |
| de    | German             |     [40,648,529](bld/top_sentences/de_top_sentences.csv) |      [218,781,994](bld/top_words/de_top_words.csv) |
| el    | Greek              |    [120,254,008](bld/top_sentences/el_top_sentences.csv) |      [639,221,011](bld/top_words/el_top_words.csv) |
| en    | English            |    [424,332,313](bld/top_sentences/en_top_sentences.csv) |    [2,558,565,196](bld/top_words/en_top_words.csv) |
| eo    | Esperanto          |         [91,573](bld/top_sentences/eo_top_sentences.csv) |          [445,995](bld/top_words/eo_top_words.csv) |
| es    | Spanish            |    [211,510,220](bld/top_sentences/es_top_sentences.csv) |    [1,137,050,844](bld/top_words/es_top_words.csv) |
| et    | Estonian           |     [27,378,143](bld/top_sentences/et_top_sentences.csv) |      [125,927,295](bld/top_words/et_top_words.csv) |
| eu    | Basque             |      [1,011,460](bld/top_sentences/eu_top_sentences.csv) |        [4,226,325](bld/top_words/eu_top_words.csv) |
| fa    | Persian            |     [12,361,713](bld/top_sentences/fa_top_sentences.csv) |       [64,326,270](bld/top_words/fa_top_words.csv) |
| fi    | Finnish            |     [51,726,104](bld/top_sentences/fi_top_sentences.csv) |      [202,514,234](bld/top_words/fi_top_words.csv) |
| fr    | French             |    [105,626,854](bld/top_sentences/fr_top_sentences.csv) |      [639,468,672](bld/top_words/fr_top_words.csv) |
| gl    | Galician           |        [304,788](bld/top_sentences/gl_top_sentences.csv) |        [1,844,553](bld/top_words/gl_top_words.csv) |
| he    | Hebrew             |     [84,803,287](bld/top_sentences/he_top_sentences.csv) |      [400,628,507](bld/top_words/he_top_words.csv) |
| hi    | Hindi              |        [125,360](bld/top_sentences/hi_top_sentences.csv) |          [765,808](bld/top_words/hi_top_words.csv) |
| hr    | Croatian           |    [112,249,406](bld/top_sentences/hr_top_sentences.csv) |      [538,052,737](bld/top_words/hr_top_words.csv) |
| hu    | Hungarian          |    [102,656,500](bld/top_sentences/hu_top_sentences.csv) |      [451,839,266](bld/top_words/hu_top_words.csv) |
| hy    | Armenian           |          [2,430](bld/top_sentences/hy_top_sentences.csv) |           [24,197](bld/top_words/hy_top_words.csv) |
| id    | Indonesian         |     [22,396,206](bld/top_sentences/id_top_sentences.csv) |      [103,580,137](bld/top_words/id_top_words.csv) |
| is    | Icelandic          |      [1,930,726](bld/top_sentences/is_top_sentences.csv) |        [9,578,051](bld/top_words/is_top_words.csv) |
| it    | Italian            |    [103,588,080](bld/top_sentences/it_top_sentences.csv) |      [581,515,381](bld/top_words/it_top_words.csv) |
| ja    | Japanese           |      [3,034,473](bld/top_sentences/ja_top_sentences.csv) |       [18,494,956](bld/top_words/ja_top_words.csv) |
| ka    | Georgian           |        [274,057](bld/top_sentences/ka_top_sentences.csv) |        [1,261,959](bld/top_words/ka_top_words.csv) |
| kk    | Kazakh             |          [4,051](bld/top_sentences/kk_top_sentences.csv) |           [14,257](bld/top_words/kk_top_words.csv) |
| ko    | Korean             |      [2,062,345](bld/top_sentences/ko_top_sentences.csv) |        [7,408,813](bld/top_words/ko_top_words.csv) |
| lt    | Lithuanian         |      [2,110,571](bld/top_sentences/lt_top_sentences.csv) |        [8,178,690](bld/top_words/lt_top_words.csv) |
| lv    | Latvian            |        [612,122](bld/top_sentences/lv_top_sentences.csv) |        [2,565,490](bld/top_words/lv_top_words.csv) |
| mk    | Macedonian         |      [7,707,280](bld/top_sentences/mk_top_sentences.csv) |       [38,379,332](bld/top_words/mk_top_words.csv) |
| ml    | Malayalam          |        [505,786](bld/top_sentences/ml_top_sentences.csv) |        [1,752,604](bld/top_words/ml_top_words.csv) |
| ms    | Malay              |      [3,769,707](bld/top_sentences/ms_top_sentences.csv) |       [17,475,629](bld/top_words/ms_top_words.csv) |
| nl    | Dutch              |    [103,995,910](bld/top_sentences/nl_top_sentences.csv) |      [595,792,461](bld/top_words/nl_top_words.csv) |
| no    | Norwegian          |     [12,866,036](bld/top_sentences/no_top_sentences.csv) |       [66,979,416](bld/top_words/no_top_words.csv) |
| pl    | Polish             |    [233,638,062](bld/top_sentences/pl_top_sentences.csv) |    [1,049,551,703](bld/top_words/pl_top_words.csv) |
| pt    | Portuguese         |    [117,679,690](bld/top_sentences/pt_top_sentences.csv) |      [623,827,834](bld/top_words/pt_top_words.csv) |
| pt_br | Portuguese, Brazil | [250,231,504](bld/top_sentences/pt_br_top_sentences.csv) | [1,313,425,238](bld/top_words/pt_br_top_words.csv) |
| ro    | Romanian           |    [191,620,920](bld/top_sentences/ro_top_sentences.csv) |    [1,051,216,598](bld/top_words/ro_top_words.csv) |
| ru    | Russian            |     [43,563,555](bld/top_sentences/ru_top_sentences.csv) |      [213,758,183](bld/top_words/ru_top_words.csv) |
| si    | Sinhala            |        [943,726](bld/top_sentences/si_top_sentences.csv) |        [4,266,309](bld/top_words/si_top_words.csv) |
| sk    | Slovak             |     [15,958,574](bld/top_sentences/sk_top_sentences.csv) |       [77,096,449](bld/top_words/sk_top_words.csv) |
| sl    | Slovenian          |     [59,309,241](bld/top_sentences/sl_top_sentences.csv) |      [267,371,023](bld/top_words/sl_top_words.csv) |
| sq    | Albanian           |      [3,549,383](bld/top_sentences/sq_top_sentences.csv) |       [18,697,836](bld/top_words/sq_top_words.csv) |
| sr    | Serbian            |    [165,175,285](bld/top_sentences/sr_top_sentences.csv) |      [807,672,359](bld/top_words/sr_top_words.csv) |
| sv    | Swedish            |     [35,955,299](bld/top_sentences/sv_top_sentences.csv) |      [188,647,795](bld/top_words/sv_top_words.csv) |
| ta    | Tamil              |         [34,263](bld/top_sentences/ta_top_sentences.csv) |          [141,693](bld/top_words/ta_top_words.csv) |
| te    | Telugu             |         [24,027](bld/top_sentences/te_top_sentences.csv) |          [107,890](bld/top_words/te_top_words.csv) |
| th    | Thai               |      [8,530,650](bld/top_sentences/th_top_sentences.csv) |       [54,028,834](bld/top_words/th_top_words.csv) |
| tl    | Tagalog            |         [18,487](bld/top_sentences/tl_top_sentences.csv) |          [103,149](bld/top_words/tl_top_words.csv) |
| tr    | Turkish            |    [172,028,191](bld/top_sentences/tr_top_sentences.csv) |      [694,495,389](bld/top_words/tr_top_words.csv) |
| uk    | Ukrainian          |      [1,199,790](bld/top_sentences/uk_top_sentences.csv) |        [5,654,100](bld/top_words/uk_top_words.csv) |
| ur    | Urdu               |         [38,672](bld/top_sentences/ur_top_sentences.csv) |          [266,345](bld/top_words/ur_top_words.csv) |
| vi    | Vietnamese         |      [5,069,885](bld/top_sentences/vi_top_sentences.csv) |       [30,297,828](bld/top_words/vi_top_words.csv) |
| ze_en | English, ze        |   [6,282,966](bld/top_sentences/ze_en_top_sentences.csv) |    [42,270,214](bld/top_words/ze_en_top_words.csv) |
| ze_zh | Chinese, ze        |   [7,093,112](bld/top_sentences/ze_zh_top_sentences.csv) |    [59,730,730](bld/top_words/ze_zh_top_words.csv) |
| zh_cn | Chinese            |  [27,167,013](bld/top_sentences/zh_cn_top_sentences.csv) |   [231,881,660](bld/top_words/zh_cn_top_words.csv) |
| zh_tw | Chinese, Taiwan    |   [9,799,690](bld/top_sentences/zh_tw_top_sentences.csv) |    [79,287,019](bld/top_words/zh_tw_top_words.csv) |

Here "ze" stands for subtitle files containing dual Chinese and English subtitles.


## The underlying corpus

This repository is based on the OpenSubtitles2018 corpus, which is part of the OPUS collection. In particular, as primary source files the untokenized corpus files linked in the rightmost column language IDs of the first table [here](https://opus.nlpl.eu/OpenSubtitles-v2018.php) are taken. These contain the raw corpus text as a collection of xml subtitle files, which have been downloaded from [www.opensubtitles.org](https://www.opensubtitles.org) (see also their newer site [www.opensubtitles.com](https://www.opensubtitles.com)). Optionally, one can also use the pre-parsed and pre-tokenized source data files.

[OpenSubtitles](https://www.opensubtitles.org) is an online community where anyone can upload and download subtitles. At the time of writing 6,404,981 subtitles are available, of these 3,735,070 are included in the [OpenSubtitles2018 corpus](https://opus.nlpl.eu/OpenSubtitles-v2018.php), which contains a total of 22.10 billion tokens (see the first columns in the second table of the preceding link for a per-language breakdown). See the following article for a detailed description of the corpus:

> P. Lison and J. Tiedemann (2016) [OpenSubtitles2016: Extracting Large Parallel Corpora from Movie and TV Subtitles](http://www.lrec-conf.org/proceedings/lrec2016/pdf/947_Paper.pdf). In Proceedings of the 10th International Conference on Language Resources and Evaluation (LREC 2016).


## Python code

### Usage

First download this repository and install the dependencies listed in [pyproject.toml](pyproject.toml) (Python 3.8+ and a few Python packages). If you're using [Poetry](https://python-poetry.org) this can be done automatically by running `poetry install --all-extras` from the command line when at the root directory of the downloaded repository. This also installs extra tokenizers for Japanese, Thai, and Vietnamese. If you only are interested in sentences or only plan to use a simpler regex tokenizer, a minimal install with `poetry install --without words` will also do.

Next adjust the *Settings* section of [src/top_open_subtitles_sentences.py](src/top_open_subtitles_sentences.py) to your liking, while optionally reading the *Info* section, before running the whole file. 


### Features

All 62 languages are supported (see the *Info* section of [src/top_open_subtitles_sentences.py](src/top_open_subtitles_sentences.py) for the full list).

The Python code will by default first download the source data, parse it into a temporary file, and then construct the lists of most common sentences and (optionally) words from it. The lists also include the number of times each sentence or word occurs in the corpus.

The following cleaning steps are performed (uncomment the respective lines in the code to skip them):

- Whitespace, dashes, and other undesirable characters are stripped from the beginning and end of each sentence.
- Entries consisting of only punctuation and numbers are removed.
- Entries starting with a parenthesis are removed as these don't contain speech. Same for entries ending with a colon.
- Entries containing Latin characters are removed for languages not using the Latin alphabet.
- Sentences ending the same way up to " .?!¿¡" are combined into the most common form.
- Words of differing cases are combined.
- Sentences in [src/extra_settings/extra_sentences_to_exclude.csv](src/extra_settings/extra_sentences_to_exclude.csv) are excluded.

[spaCy](https://spacy.io) is used to split sentences into words (tokenization).


## Related work

There exist at least two similar but older repositories (older versions of the corpus and older Python versions) which can parse the OpenSubtitles corpus but do not extract lists of the most frequent sentences and words: [AlJohri/OpenSubtitles](https://github.com/AlJohri/OpenSubtitles) and [domerin0/opensubtitles-parser](https://github.com/domerin0/opensubtitles-parser).

The [orgtre/google-books-ngram-frequency](https://github.com/orgtre/google-books-ngram-frequency) repository constructs similar lists of the most frequent words and sequences of up to five words (ngrams) in the much larger Google Books Ngram Corpus.


## Limitations and known problems

If a movie or episode contains several different subtitle files for a given language in the raw corpus, then all of them are used when constructing the sentence/word lists. Since more popular movies and episodes tend to have more subtitle files, the resulting lists can hence be viewed as popularity-weighted. #TODO Add option to only include one subtitle per movie.

Many of the subtitle files are not in the same language as the movie they are subtitling. They are hence translations, often from English, and don't represent typical sentence and word usage in the subtitle language. #TODO add option to filter out only subtitles whose language is the same as in the movie.

The sentence and word lists still contain entries which probably better are excluded for many purposes, like proper names. #TODO clean the lists more.

The code in this repository is licensed under the [Creative Commons Attribution 3.0 Unported License](https://creativecommons.org/licenses/by/3.0/). The sentence and word lists come with the same license as [the underlying corpus](http://opus.nlpl.eu/OpenSubtitles-v2018.php). Issue reports and pull requests (especially for #TODO tasks) are most welcome!
