## Multi 'headed' scraper factory

Utilizes GCP pub/sub, cloud functions and a singule flask application controller hosted on cloud run
to systematically and *quickly* retrieve pricing information from the source. If/when rate limits are hit
that particular 'head' will submit both its successfully harvested payload as well as the remainder of its task queue
to the appropriate channels, ensuring data is not lost and the spawning of a new process in a different region.

the included shell script *should* successfully create and connect all required components, assuming ADC/permissions are set up,
as well as that the provided big query resource exists (or is replaced with a different implementation that does the same thing)

## Note: I have yet to test this (lol) -- slated for tomorrow, probably (written on 3/3/24)