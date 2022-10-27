def get_rank_corr(langcode, trans_base_dir, orig_base_dir,
                  ntop=1000, for_words=False, write_data=True):
    if for_words:
        entry = "word"
    else:
        entry = "sentence"
    
    dt = (pd.read_csv(f"{trans_base_dir}top_{entry}s/"
                      + f"{langcode}_top_{entry}s.csv")
          .assign(**{f"{entry}": lambda x: x[entry].str.strip(" .?!¿¡")})
          .assign(**{f"{entry}": lambda x: x[entry].str.lower()})
          .groupby(by=[entry]).sum()
          .sort_values(by=["count"], ascending=False)
          .reset_index()
          .assign(rankt = lambda x: x.index)
          .drop(columns="count")
          .drop_duplicates(subset=entry, keep=False))

    do = (pd.read_csv(f"{orig_base_dir}top_{entry}s/"
                      + f"{langcode}_top_{entry}s.csv")
          .assign(**{f"{entry}": lambda x: x[entry].str.strip(" .?!¿¡")})
          .assign(**{f"{entry}": lambda x: x[entry].str.lower()})          
          .groupby(by=[entry]).sum()
          .sort_values(by=["count"], ascending=False)
          .reset_index()
          .assign(ranko = lambda x: x.index)
          .drop(columns="count")
          .drop_duplicates(subset=entry, keep=False))

    dm = dt.merge(do, on=entry, how="left")
    dm["ranko"] = pd.to_numeric(dm["ranko"].fillna(len(dm)), downcast="integer")

    if write_data:
        dm.to_csv(f"{orig_base_dir}{langcode}_{entry}_rank_comparison.csv")
    
    corr_fillna = (dm[0:ntop].corr(numeric_only=True)
                   .loc["rankt", "ranko"])
    corr_omitna = (dm[dm.ranko != len(dm)][0:ntop]
                   .corr(numeric_only=True).loc["rankt", "ranko"])
    return {"corr_fillna": corr_fillna, "corr_omitna": corr_omitna}


get_rank_corr(langcode="en",
              trans_base_dir="bld/",
              orig_base_dir="bld/original_language_only/",
              ntop=1000, for_words=False)

get_rank_corr(langcode="en",
              trans_base_dir="bld/",
              orig_base_dir="bld/original_language_only/",
              ntop=1000, for_words=True)

get_rank_corr(langcode="es",
              trans_base_dir="bld/",
              orig_base_dir="bld/original_language_only/",
              ntop=1000, for_words=False)

get_rank_corr(langcode="es",
              trans_base_dir="bld/",
              orig_base_dir="bld/original_language_only/",
              ntop=1000, for_words=True)
