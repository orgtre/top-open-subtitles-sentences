# Build the top-open-subtitles-sentences repository

import os
import pandas as pd
import itertools
import nltk
from nltk import FreqDist
# nltk.download('punkt')
import time
import requests
import zipfile
import io


###############################################################################
# Settings

langcode = "ar"
year_min = 0
year_max = 2018

get_raw_data = True
get_parsed_text = True
get_sentences = True
get_words = True

n_top_sentences = 10000
n_top_words = 30000

basedatadir = "src/data"
datadir = f"src/data/{langcode}/OpenSubtitles/raw/{langcode}"
tmpfile = f"bld/{langcode}_parsed_text.txt"
sentence_outfile = f"bld/{langcode}_top_sentences.txt"
word_outfile = f"bld/{langcode}_top_words.txt"

extra_sentences_to_exclude = \
    (pd.read_csv(f"src/extra_settings/extra_sentences_to_exclude.csv")
     .to_dict('list'))


###############################################################################
# Info

# Valid langcodes are:
# af, ar, bg, bn, br, bs, ca, cs, da, de, el, en, eo, es, et, eu, fa, fi, fr,
# gl, he, hi, hr, hu, hy, id, is, it, ja, ka, kk, ko, lt, lv, mk, ml, ms, nl,
# no, pl, pt, pt_br, ro, ru, si, sk, sl, sq, sr, sv, ta, te, th, tl, tr, uk,
# ur, vi, ze_en, ze_zh, zh_cn, zh_tw

# Storage requirements:
# When langcode = "nl", the extracted raw corpus data takes up 12.7GB.

# Memory requirements:
# When langcode = "nl", year_min = 0, and year_max = 2018, the corpus is parsed
# into a file of 3.32GB. This whole file is then loaded into memory and the
# python code requires more than 19GB when extracting top sentences.

# Download time:
# I get download speeds of around 50MB/s. The zipped raw corpus for a large
# language like "en" is around 13GB and hence takes me around 4 minutes to
# download.

# Runtime excluding data download (on M1 MBP):
# When langcode = "nl", year_min = 0, year_max = 2018, get_words = False:
# 23 minutes.
# With get_words = True, running for only year 2016 takes 240s.


###############################################################################
# Constants etc.

source_zipfile = ("https://opus.nlpl.eu/download.php?f="
                  + f"OpenSubtitles/v2018/raw/{langcode}.zip")


###############################################################################
# Functions

def download_data_file(url, basedatadir, langcode):
    local_filename = os.path.join(basedatadir, f"{langcode}.zip")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1000000): 
                f.write(chunk)
    return local_filename


def download_data_and_extract(basedatadir, langcode):
    f = download_data_file(source_zipfile, basedatadir, langcode)
    with zipfile.ZipFile(f, 'r') as zip_ref:
        zip_ref.extractall(os.path.join(basedatadir, f"{langcode}"))
    os.remove(f)


def parse_datadir_to_tmpfile(datadir, tmpfile, year_min, year_max):
    print("parsing data")
    if os.path.exists(tmpfile):
        os.remove(tmpfile)
    fout = open(tmpfile, 'a')
    for ydir in os.listdir(datadir):
        try:
            if int(ydir) >= year_min and int(ydir) <= year_max:
                print(ydir)
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
                text += line                
    fin.close()
    return text


def tmpfile_to_top_sentences(tmpfile, outfile):
    print("getting top sentences")
    with open(tmpfile, 'r') as file:
        lines = file.read().splitlines()
    
    d = pd.DataFrame(lines, columns=["sentence"])
    d = d.groupby(['sentence'])['sentence'].count()
    d = d.sort_values(ascending=False)
    d = d.to_frame(name="count").reset_index()

    d['sentence'] = d['sentence'].str.replace("^- ", "" , regex=True)
    d['sentence'] = d['sentence'].str.replace("^-", "" , regex=True)
    d['sentence'] = d['sentence'].str.strip()

    d = d.groupby(['sentence'])['count'].sum()
    d = d.sort_values(ascending=False)
    d = d.to_frame(name="count").reset_index()

    d = collapse_if_only_ending_differently(d, "sentence", "count")

    try:
        d = d[~d['sentence'].isin(extra_sentences_to_exclude[langcode])]
    except KeyError as e:
        print("No extra sentences to exclude found.")
    
    (d
     .head(n_top_sentences)
     .to_csv(outfile, index=False))


def collapse_if_only_ending_differently(df, sentence, count):
    return (df
            .sort_values(by=[count], ascending=False)
            .assign(Sm1=df[sentence].str[:-1])
            .groupby('Sm1', as_index=False)
            .agg({sentence:'first', count:'sum'})
            .drop(columns=['Sm1'])
            .sort_values(by=[count], ascending=False)
            .reset_index(drop=True))


def collapse_case(df, word, count):
    return (df
            .sort_values(by=[count], ascending=False)
            .assign(Slow=df[word].str.lower())
            .groupby('Slow', as_index=False)
            .agg({word:'first', count:'sum'})
            .drop(columns=['Slow'])
            .sort_values(by=[count], ascending=False)
            .reset_index(drop=True))
    

def tmpfile_to_top_words(tmpfile, outfile):
    print("getting top words")
    with open(tmpfile, 'r') as file:
        lines = file.read().splitlines()
    d = map(nltk.word_tokenize, lines)
    d = (pd.DataFrame({'word': itertools.chain.from_iterable(d)})
         .value_counts()
         .reset_index(name="count"))

    d = collapse_case(d, "word", "count")
    
    punctuation_and_numbers_regex = r"^[ _\W0-9]+$"
    d = d[~d.word.str.contains(punctuation_and_numbers_regex)]

    #TODO add more cleaning steps from google-books-ngram-frequency repo
    
    (d
     .head(n_top_words)
     .to_csv(outfile, index=False))
    

###############################################################################
# Run

t0 = time.time()
if get_raw_data:
   download_data_and_extract(basedatadir, langcode)
if get_parsed_text:
    parse_datadir_to_tmpfile(datadir, tmpfile, year_min, year_max)
if get_sentences:
    tmpfile_to_top_sentences(tmpfile, sentence_outfile)
if get_words:
    tmpfile_to_top_words(tmpfile, word_outfile)
t1 = time.time()
print(f"total time (s): {t1-t0}")


###############################################################################
# Misc

# import time

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
# t0 = time.time()
# dt = FreqDist(nltk.word_tokenize(text[0:1000000]))
# dt = (pd.DataFrame.from_dict(dt.items())
#       .rename(columns={0: "word", 1: "count"})
#       .sort_values(by="count", ascending=False)
#       .reset_index(drop=True))
# t1 = time.time()
# total3 = t1-t0

# method 2 and 3 take the same time and are around 3 times slower than 1
# with method 1, year 2017 takes around 100s
