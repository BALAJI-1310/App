if Phone_Updated:
    df_AccountProfile_filtered = df_AccountProfile.filter(
        f.regexp_replace(
            f.regexp_replace(f.trim(f.col("MainPhoneNumber")), "^\\+", ""),
            "\\s", ""
        ) != Phone_Updated
    )
else:
    df_AccountProfile_filtered = df_AccountProfile
