/* Example of mdi_purge_analyze_fact.sas */

/* Define the library where the data resides */
libname mylib 'path_to_your_data';

/* Start the data purge process */
proc sql;
    /* Display the number of records before deletion */
    select count(*) as records_before
    from mylib.your_table;

    /* Delete records where the date is before January 1, 2023 */
    delete from mylib.your_table
    where record_date < '01JAN2023'd;

    /* Display the number of records after deletion */
    select count(*) as records_after
    from mylib.your_table;
quit;

/* Optionally log the results */
data _null_;
    set mylib.your_table end=last;
    if last then do;
        put 'Total records remaining: ' _n_;
    end;
run;
