# Datasets

## Wave I

Descriptions and extraction dates for datasets used in the WikiProjects project.

1. **All pages** listing from the Wikipedia API for the following namespaces:
    * 4 and 5 (around 6 January 2020)
    * 0, 1, 2 and 3 (around 5 February 2020)
2. **Cirrus docs** for WP pages and userpages of users involed in WikiProjects
   (namespaces 2, 3, 4, 5). About 850 000 documents.
   Downloaded around 8 February 2020 (WP) and 2 March 2020 (userpages).
3. **Posts on WP pages** parsed form cirrus doc source code. Based on the
   data from point 2. Only project namespace (`ns=4`) was included.
4. **Posts on userpages of selected users** parsed from cirrus doc source code.
   Based on the data from point 2. Both man and talk was used
   (`ns=2` or `ns=3`).
5. **Users** listing from the Wikipedia API for a set of users posting
   on WP pages. Based on posts from point 3. Collected around 11 March 2020.
6. **Page assessments** listing from the Wikipedia API for all articles
   (pages in `ns=0`). Collected around 12 March 2020.
7. **Direct messages to users involved in WP**. Posts were parsed
   from cirrus source code of userpages of selected users. Collected in point 4.
