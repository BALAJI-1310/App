if row_count > 0 and Operation_Updated == "delete":
    # Filter `df_ContactProfile` by removing rows matching the conditions
    if Email_Updated and Phone_Updated:
        df_ContactProfile_filtered = df_ContactProfile.filter(
            ~(
                (f.lower(f.trim(f.col("EmailAddress"))) == Email_Updated) |
                (f.regexp_replace(f.regexp_replace(f.trim(f.col("ContactPhoneNumber")), "^\\+", ""), "\\s", "") == Phone_Updated) |
                (f.regexp_replace(f.regexp_replace(f.trim(f.col("OrganizationPhoneNumber")), "^\\+", ""), "\\s", "") == Phone_Updated)
            )
        )
    elif Email_Updated:
        df_ContactProfile_filtered = df_ContactProfile.filter(
            f.lower(f.trim(f.col("EmailAddress"))) != Email_Updated
        )
    elif Phone_Updated:
        df_ContactProfile_filtered = df_ContactProfile.filter(
            (f.regexp_replace(f.regexp_replace(f.trim(f.col("ContactPhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated) &
            (f.regexp_replace(f.regexp_replace(f.trim(f.col("OrganizationPhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated)
        )
    else:
        df_ContactProfile_filtered = df_ContactProfile

    # Filter `df_AccountProfile` similarly
    if Phone_Updated:
        df_AccountProfile_filtered = df_AccountProfile.filter(
            f.regexp_replace(f.regexp_replace(f.trim(f.col("MainPhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated
        )
    else:
        df_AccountProfile_filtered = df_AccountProfile

    # Filter `df_individualalternatekey` similarly
    if Email_Updated:
        df_individualalternatekey_filtered = df_individualalternatekey.filter(
            f.lower(f.trim(f.col("EmailAddress"))) != Email_Updated
        )
        df_individualalternatekey_edl_filtered = df_individualalternatekey_edl.filter(
            f.lower(f.trim(f.col("EmailAddress"))) != Email_Updated
        )
    else:
        df_individualalternatekey_filtered = df_individualalternatekey
        df_individualalternatekey_edl_filtered = df_individualalternatekey_edl

    # Filter `df_dimorganizationprofile` similarly

    if Phone_Updated:
        df_dimorganizationprofile_filtered = df_dimorganizationprofile.filter(
            f.regexp_replace(f.regexp_replace(f.trim(f.col("MainPhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated
        )
        df_dimorganizationprofile_edl_filtered = df_dimorganizationprofile_edl.filter(
            f.regexp_replace(f.regexp_replace(f.trim(f.col("MainPhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated
        )
    else:
        df_dimorganizationprofile_filtered = df_dimorganizationprofile
        df_dimorganizationprofile_edl_filtered = df_dimorganizationprofile_edl

    # Filter `df_dimindividualprofile` similarly
    if Email_Updated and Phone_Updated:
        df_dimindividualprofile_filtered = df_dimindividualprofile.filter(
            ~(
                (f.lower(f.trim(f.col("EmailAddress"))) == Email_Updated) |
                (f.regexp_replace(f.regexp_replace(f.trim(f.col("ContactPhoneNumber")), "^\\+", ""), "\\s", "") == Phone_Updated) |
                (f.regexp_replace(f.regexp_replace(f.trim(f.col("StandardizedContactPhoneNumber")), "^\\+", ""), "\\s", "") == Phone_Updated) |
                (f.regexp_replace(f.regexp_replace(f.trim(f.col("MobilePhoneNumber")), "^\\+", ""), "\\s", "") == Phone_Updated) |
                (f.regexp_replace(f.regexp_replace(f.trim(f.col("OrganizationPhoneNumber")), "^\\+", ""), "\\s", "") == Phone_Updated) |
                (f.regexp_replace(f.regexp_replace(f.trim(f.col("StandardizedOrganizationPhoneNumber")), "^\\+", ""), "\\s", "") == Phone_Updated)
            )
        )
    elif Email_Updated:
        df_dimindividualprofile_filtered = df_dimindividualprofile.filter(
            f.lower(f.trim(f.col("EmailAddress"))) != Email_Updated
        )
    elif Phone_Updated:
        df_dimindividualprofile_filtered = df_dimindividualprofile.filter(
            (f.regexp_replace(f.regexp_replace(f.trim(f.col("ContactPhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated) &
            (f.regexp_replace(f.regexp_replace(f.trim(f.col("StandardizedContactPhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated) &
            (f.regexp_replace(f.regexp_replace(f.trim(f.col("MobilePhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated) &
            (f.regexp_replace(f.regexp_replace(f.trim(f.col("OrganizationPhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated) &
            (f.regexp_replace(f.regexp_replace(f.trim(f.col("StandardizedOrganizationPhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated)
        )
    else:
        df_dimindividualprofile_filtered = df_dimindividualprofile

    if Email_Updated and Phone_Updated:
        df_dimindividualprofile_edl_filtered = df_dimindividualprofile_edl.filter(
            ~(
                (f.lower(f.trim(f.col("EmailAddress"))) == Email_Updated) |
                (f.regexp_replace(f.regexp_replace(f.trim(f.col("ContactPhoneNumber")), "^\\+", ""), "\\s", "") == Phone_Updated) |
                (f.regexp_replace(f.regexp_replace(f.trim(f.col("StandardizedContactPhoneNumber")), "^\\+", ""), "\\s", "") == Phone_Updated) |
                (f.regexp_replace(f.regexp_replace(f.trim(f.col("MobilePhoneNumber")), "^\\+", ""), "\\s", "") == Phone_Updated) |
                (f.regexp_replace(f.regexp_replace(f.trim(f.col("OrganizationPhoneNumber")), "^\\+", ""), "\\s", "") == Phone_Updated) |
                (f.regexp_replace(f.regexp_replace(f.trim(f.col("StandardizedOrganizationPhoneNumber")), "^\\+", ""), "\\s", "") == Phone_Updated)
            )
        )
    elif Email_Updated:
        df_dimindividualprofile_edl_filtered = df_dimindividualprofile_edl.filter(
            f.lower(f.trim(f.col("EmailAddress"))) != Email_Updated
        )
    elif Phone_Updated:
        df_dimindividualprofile_edl_filtered = df_dimindividualprofile_edl.filter(
            (f.regexp_replace(f.regexp_replace(f.trim(f.col("ContactPhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated) &
            (f.regexp_replace(f.regexp_replace(f.trim(f.col("StandardizedContactPhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated) &
            (f.regexp_replace(f.regexp_replace(f.trim(f.col("MobilePhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated) &
            (f.regexp_replace(f.regexp_replace(f.trim(f.col("OrganizationPhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated) &
            (f.regexp_replace(f.regexp_replace(f.trim(f.col("StandardizedOrganizationPhoneNumber")), "^\\+", ""), "\\s", "") != Phone_Updated)
        )
    else:
        df_dimindividualprofile_edl_filtered = df_dimindividualprofile_edl
