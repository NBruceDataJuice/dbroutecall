# dbroutecall
NiFi - This is to be used within the NiFi Error Retry framework referred to here: https://www.datajuice.io/post/nifi-error-retry-framework-intro

The purpose is to make a call to a database and retrieve the appropriate route given the processor, progress group, and number of attempts. This is needed because at the time of development, all processors packaged with NiFi that are able to make a database call, overwrite the contents of the file.

Usage:
The schema needs to be created and populated in any relational database in which the SQL statement is compliant (I think it will work in all major distributions). There must be a DBCP Service set in the processor for it to operate correctly. 
