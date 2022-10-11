# Build the top-open-subtitles-sentences repository

import os
import pandas as pd
import itertools
import nltk
from nltk import FreqDist
# nltk.download('punkt')
import time


###############################################################################
# Settings

langcode = "nl"
year_min = 2016
year_max = 2016
get_sentences = True
get_words = True
n_top_sentences = 10000
n_top_words = 30000
sentence_outfile = "bld/top_sentences.txt"
word_outfile = "bld/top_words.txt"
tmpfile = "bld/parsed_text.txt"
datadir = "src/data/nl/OpenSubtitles/raw/nl"

extra_sentences_to_exclude = \
    (pd.read_csv(f"src/extra_settings/extra_sentences_to_exclude.csv")
     .to_dict('list'))


###############################################################################
# Functions

# TODO: code to download data

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

    d = d[~d['sentence'].isin(extra_sentences_to_exclude[langcode])]
    
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
parse_datadir_to_tmpfile(datadir, tmpfile, year_min, year_max)
if get_sentences:
    tmpfile_to_top_sentences(tmpfile, sentence_outfile)
if get_words:
    tmpfile_to_top_words(tmpfile, word_outfile)
t1 = time.time()
print(f"total time (s): f{t1-t0}")
# Running for only year 2016 takes 240s


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
