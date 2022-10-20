# Build the top-open-subtitles-sentences repository

import os
import pandas as pd
import itertools
from collections import Counter
import nltk
# nltk.download('punkt')
import time
import requests
import zipfile
import re


###############################################################################
# Settings

# subtitle languages and years
langcodes = ["en"]   # see valid_langcodes
year_min = 0   # lowest: 0
year_max = 2018   # largest: 2018

# parts to run
get_raw_data = True
get_parsed_text = True
get_sentences = True
get_words = False

# performance
download_chunk_size = 1000000
min_count = 5
lines_per_chunk = 10000000

# output settings
n_top_sentences = 10000
n_top_words = 30000


###############################################################################
# Info

# Valid langcodes:
# See the variable 'valid_langcodes' below. For a key see
# https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes
# but note that some codes contain additional suffixes and that 'ze' signifies
# files containing dual Chinese and English subtitles.

# Storage requirements:
# Size of the extracted raw corpus data by langcode:
# "en": 54.2GB, "es": 27.1GB, "nl": 12.7GB

# Memory requirements:
# When langcode = "nl", year_min = 0, and year_max = 2018, the corpus is parsed
# into a file of 3.32GB. This file is then loaded 'lines_per_chunk' lines at a
# time into a Counter (dict subclass) object which still might take many GBs of
# memory. By setting 'min_count', entries with count less than that can be
# omitted to save memory, but this only happens after the whole tempfile has
# been loaded (otherwise the final counts would not be correct).

# Download time:
# Variable 'download_chunk_size' influences the download time. With the
# default I get download speeds of around 50MB/s. The zipped raw corpus for a
# large language like "en" is around 13GB and hence takes me around 4 minutes
# to download.

# Runtime excluding data download (on M1 MBP):
# When langcode = "nl", year_min = 0, year_max = 2018:
# 4 minutes with get_words = False.
# 1h26min with get_words = True.


###############################################################################
# Constants etc.

valid_langcodes = ["af", "ar", "bg", "bn", "br", "bs", "ca", "cs", "da", "de",
                   "el", "en", "eo", "es", "et", "eu", "fa", "fi", "fr", "gl",
                   "he", "hi", "hr", "hu", "hy", "id", "is", "it", "ja", "ka",
                   "kk", "ko", "lt", "lv", "mk", "ml", "ms", "nl", "no", "pl",
                   "pt", "pt_br", "ro", "ru", "si", "sk", "sl", "sq", "sr",
                   "sv", "ta", "te", "th", "tl", "tr", "uk", "ur", "vi",
                   "ze_en", "ze_zh", "zh_cn", "zh_tw"]

def source_zipfile(langcode):
    return ("https://opus.nlpl.eu/download.php?f="
            + f"OpenSubtitles/v2018/raw/{langcode}.zip")

basedatadir = "src/data"

def datadir(langcode):
    return f"{basedatadir}/{langcode}/OpenSubtitles/raw/{langcode}"

def tmpfile(langcode):
    return f"bld/{langcode}_parsed_text.txt"

def sentence_outfile(langcode):
    return f"bld/{langcode}_top_sentences.txt"

def word_outfile(langcode):
    return f"bld/{langcode}_top_words.txt"

def extra_sentences_to_exclude():
    return (pd.read_csv(f"src/extra_settings/extra_sentences_to_exclude.csv")
            .to_dict('list'))
    

###############################################################################
# Functions

def download_data_and_extract(basedatadir, langcode):
    print("Downloading data:")
    f = download_data_file(source_zipfile(langcode), basedatadir, langcode)
    with zipfile.ZipFile(f, 'r') as zip_ref:
        zip_ref.extractall(os.path.join(basedatadir, f"{langcode}"))
    os.remove(f)


def download_data_file(url, basedatadir, langcode):
    local_filename = os.path.join(basedatadir, f"{langcode}.zip")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_length = int(r.headers.get('content-length'))
        print(f"   downloading {total_length/1e6:.1f} MB...")
        print_period = max(round(total_length/download_chunk_size/10), 1)        
        download_length = 0
        i = 1
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=download_chunk_size):
                download_length += len(chunk)
                f.write(chunk)
                if i % print_period == 0:
                    print(f"   {download_length/total_length*100:.0f}% done")
                i += 1
    return local_filename


def parse_datadir_to_tmpfile(datadir, tmpfile, year_min, year_max):
    print("Parsing data:")
    if os.path.exists(tmpfile):
        os.remove(tmpfile)
    fout = open(tmpfile, 'a')
    for ydir in os.listdir(datadir):
        try:
            if int(ydir) >= year_min and int(ydir) <= year_max:
                print(f"   {ydir}")
                outtext = ""
                for mdir in os.listdir(os.path.join(datadir, ydir)):
                    #print("mdir: " + mdir)
                    mdirfull = os.path.join(datadir, ydir, mdir)
                    if os.path.isdir(mdirfull):
                        for fname in os.listdir(mdirfull):
                            if not fname.startswith('.'):
                                #print(fname)
                                fpathfull = os.path.join(datadir, ydir,
                                                         mdir, fname)
                                outtext += parse_xmlfile(fpathfull)
                fout.write(outtext)
        except ValueError:
            pass
    fout.close()

    
def parse_xmlfile(infile):
    fin = open(infile, 'r')
    text = ""
    for line in fin.readlines():
        if not (line.startswith('<')):
            if not (line.startswith(' ')):
                text += line.strip(" -\n\t") + "\n"
    fin.close()
    return text


def tmpfile_to_top_sentences(tmpfile, outfile, langcode):
    print("Getting top sentences:")
    # Chunking is faster once the tmpfile is too large to fit in RAM
    # and only slightly slower when it fits in RAM.
    # The below section takes around 5min with 'es' and all years.
    with open(tmpfile, 'br') as f:
        nlines = sum(1 for i in f)
        print(f"   processing {nlines} lines...")
    d = Counter()
    chunks_done = 0
    with open(tmpfile, 'r') as f:    
        for lines in itertools.zip_longest(*[f] * min(nlines, lines_per_chunk),
                                           fillvalue=""):
            d += Counter(lines)
            chunks_done += 1
            print(f"   {chunks_done * min(nlines, lines_per_chunk)} lines done")

    # remove empty entries
    d.pop("", None)
    d.pop(None, None)

    # remove punctuation and numbers
    punctuation_and_numbers_regex = r"^[ _\W0-9]+$"
    d = Counter({k:v for (k, v) in d.items() if not
                 re.match(punctuation_and_numbers_regex, k)})

    # TODO make use of this
    total_count = sum(d.values())
    
    # remove less common items to save memory
    if not (min_count == None or min_count == 0):
        d = Counter({key:value for key, value in d.items()
                      if value >= min_count})

    # "/n" is still at the end of every sentence
    d = Counter({k.strip(): v for (k, v) in d.items()})
    # this takes the same time as instead running below:
    # d['sentence'] = d['sentence'].str.strip()

    # TODO try doing everything in a dictionary instead of using pandas
    d = pd.DataFrame(d.most_common(), columns=['sentence', 'count'])
    d['count'] = pd.to_numeric(d['count'],
                               downcast="unsigned") # saves ~50% memory
    d = d.astype({"sentence":"string[pyarrow]"}) # saves ~66% memory

    d = collapse_if_only_ending_differently(d, "sentence", "count")

    try:
        d = d[~d['sentence'].isin(extra_sentences_to_exclude()[langcode])]
    except KeyError as e:
        print("No extra sentences to exclude found.")
    
    (d
     .head(n_top_sentences)
     .to_csv(outfile, index=False))

    
def collapse_if_only_ending_differently(df, sentence, count):
    return (df
            .sort_values(by=[count], ascending=False)
            .assign(Sm1=df[sentence].str.strip(" .?!¿¡"))
            .groupby('Sm1', as_index=False)
            .agg({sentence:'first', count:'sum'})
            .drop(columns=['Sm1'])
            .sort_values(by=[count], ascending=False)
            .reset_index(drop=True))


def tmpfile_to_top_words(tmpfile, outfile):
    print("Getting top words:")
    with open(tmpfile, 'br') as f:
        nlines = sum(1 for i in f)
        print(f"   processing {nlines} lines...")
    d = Counter()
    chunks_done = 0
    with open(tmpfile, 'r') as f:            
        for lines in itertools.zip_longest(*[f] * min(nlines, lines_per_chunk),
                                           fillvalue=""):
            dt = map(nltk.word_tokenize, lines)
            d += Counter(itertools.chain.from_iterable(dt))
            chunks_done += 1
            print(f"   {chunks_done * min(nlines, lines_per_chunk)} lines done")
            # 6 min per 10,000,000 lines ("nl" has 107,000,000 lines)
            # TODO parallelize
        del dt

    # remove empty entries
    d.pop("", None)
    d.pop(None, None)

    # remove punctuation and numbers
    punctuation_and_numbers_regex = r"^[ _\W0-9]+$"
    d = Counter({k:v for (k, v) in d.items() if not
                 re.match(punctuation_and_numbers_regex, k)})

    total_count = sum(d.values())
    
    # remove less common items to save memory
    if not (min_count == None or min_count == 0):
        d = Counter({key:value for key, value in d.items()
                      if value >= min_count})

    d = pd.DataFrame(d.most_common(), columns=['word', 'count'])
    d['count'] = pd.to_numeric(d['count'],
                               downcast="unsigned") # saves ~50% memory
    d = d.astype({"word":"string[pyarrow]"}) # saves ~66% memory
    
    d = collapse_case(d, "word", "count")
    
    #TODO add more cleaning steps from google-books-ngram-frequency repo
    #e.g. remove leading punctuation (important for Spanish)
    
    (d
     .head(n_top_words)
     .to_csv(outfile, index=False))


def collapse_case(df, word, count):
    return (df
            .sort_values(by=[count], ascending=False)
            .assign(Slow=df[word].str.lower())
            .groupby('Slow', as_index=False)
            .agg({word:'first', count:'sum'})
            .drop(columns=['Slow'])
            .sort_values(by=[count], ascending=False)
            .reset_index(drop=True))
    
        
def run_one_langcode(langcode):    
    t0 = time.time()
    check_cwd()
    print("\nLanguage:", langcode)
    if get_raw_data:
        download_data_and_extract(basedatadir, langcode)
    if get_parsed_text:
        parse_datadir_to_tmpfile(datadir(langcode), tmpfile(langcode),
                                 year_min, year_max)
    if get_sentences:
        tmpfile_to_top_sentences(tmpfile(langcode), sentence_outfile(langcode),
                                 langcode)
    if get_words:
        tmpfile_to_top_words(tmpfile(langcode), word_outfile(langcode))
    t1 = time.time()
    print(f"Total time (s): {t1-t0:.1f}\n")


def check_cwd():
    if not os.path.isfile('src/top_open_subtitles_sentences.py'):
        print("Warning: 'src/top_open_subtitles_sentences.py' not found "
              + "where expected. Trying to switch to parent directory.")
        os.chdir("..")
        if not os.path.isfile('src/top_open_subtitles_sentences.py'):
            raise Exception("Error: Working directory is not the repository "
                            + "base directory. "
                            + "'src/top_open_subtitles_sentences.py' "
                            + "not found.")


def check_langcodes():
    for langcode in langcodes:
        if langcode not in valid_langcodes:
            raise Exception(f"Error: Not a valid langcode: {langcode}")
        
    
def main():
    check_langcodes()
    for langcode in langcodes:        
        run_one_langcode(langcode)

        
###############################################################################
# Run

if __name__ == "__main__":
    main()


###############################################################################
# Misc

# # method 0
# t0 = time.time()
# dt = map(nltk.word_tokenize, lines[0:1000000])
# dt = Counter(itertools.chain.from_iterable(dt))
# t1 = time.time()
# total0 = t1-t0

# # method 1
# t0 = time.time()
# dt = map(nltk.word_tokenize, lines[0:1000000])
# dt = pd.DataFrame({'word': itertools.chain.from_iterable(dt)})
# dt = (dt.value_counts()
#       .reset_index(name="count"))
# t1 = time.time()
# total1 = t1-t0

# # method 2
# t0 = time.time()
# dt = nltk.word_tokenize(text[0:1000000])
# dt = pd.DataFrame({'word': dt})
# dt = (dt.value_counts()
#       .reset_index(name="count"))
# t1 = time.time()
# total2 = t1-t0

# # method 3
# from nltk import FreqDist
# t0 = time.time()
# dt = FreqDist(nltk.word_tokenize(text[0:1000000]))
# dt = (pd.DataFrame.from_dict(dt.items())
#       .rename(columns={0: "word", 1: "count"})
#       .sort_values(by="count", ascending=False)
#       .reset_index(drop=True))
# t1 = time.time()
# total3 = t1-t0

# method 2 and 3 take the same time and are around 3 times slower than 1 and 0
# with method 1, year 2017 takes around 100s
# method 0 uses the least memory