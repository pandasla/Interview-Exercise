# Interview-Exercise

1) The email advocating is about critical performance issue with importing 100,000 of transaction data from a file using existing logic in ARM application. I think, this is a good example because this is a real live issue I faced most recently (just about a month ago) and I successfully resolved the performance issue (100 times faster).
I also included TransactionImportManager.java which has most of the solutions and codes I wrote for this issue.

2) The MoviePremierListing web page displays a list of movies coming up next month. This is written in JavaScript with JSON to parse the live movie metadata retrieved from a movie site and displays it on the html page.
 * It has a table to show the movie title, date, rated, poster, and summary.
 * Users can click on the movie title or poster to open a new page and display detailed information about the movie in IMDB.com.

Updated:
The MoviePremierListing gets cross-domain security access issue from browsers like FireFox and Chrome.
For Chrome browser, you can open it with --disable-web-security option;

On Windows:
chrome.exe --disable-web-security

On Mac:
open /Applications/Google\ Chrome.app/ --args --disable-web-security

For FireFox, MoviePremierListing gets a movie metadata from a local file, "movieData.json" (which has a JSON format movie metadata for the month of June). The "movieData.json" file should be located on the same location as the "MoviePremierListing.html".  
