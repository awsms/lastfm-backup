#!/usr/bin/env python3

import json
import urllib.error
import urllib.request
from sys import stdout, stderr
import logging
import os

import backoff

__author__ = 'Alexander Popov'
__version__ = '1.2.0'
__license__ = 'Unlicense'


STATE_FILE = "scrobbles_state.json"
SCROBBLES_FILE = "scrobbles.json"
FAVOURITES_FILE = "favourites.json"


# API helpers

def _get(url):
    """Simple helper to GET and decode JSON."""
    resp = urllib.request.urlopen(url).read().decode("utf8")
    return json.loads(resp)


def get_pages(username, api_key):
    """Total pages for recent tracks (scrobbles)."""
    data = _get(
        'https://ws.audioscrobbler.com/2.0/'
        '?method=user.getrecenttracks&user={0}&api_key={1}&format=json'
        '&limit=200'.format(username, api_key)
    )
    return int(data['recenttracks']['@attr']['totalPages'])


# http://www.last.fm/api/show/user.getRecentTracks
@backoff.on_exception(backoff.expo, urllib.error.HTTPError, max_time=60 * 60)
def get_scrobbles(username, api_key, page):
    data = _get(
        'https://ws.audioscrobbler.com/2.0/'
        '?method=user.getrecenttracks&user={0}&api_key={1}&format=json'
        '&limit=200&page={2}'.format(username, api_key, page)
    )
    return data['recenttracks']['track']


def get_loved_pages(username, api_key):
    """Total pages for loved tracks (favourites)."""
    data = _get(
        'https://ws.audioscrobbler.com/2.0/'
        '?method=user.getlovedtracks&user={0}&api_key={1}&format=json'
        '&limit=1000'.format(username, api_key)
    )
    return int(data['lovedtracks']['@attr']['totalPages'])


@backoff.on_exception(backoff.expo, urllib.error.HTTPError, max_time=60 * 60)
def get_loved_tracks(username, api_key, page):
    data = _get(
        'https://ws.audioscrobbler.com/2.0/'
        '?method=user.getlovedtracks&user={0}&api_key={1}&format=json'
        '&limit=1000&page={2}'.format(username, api_key, page)
    )
    return data['lovedtracks']['track']


# I/O helpers

def save_json(data, filename):
    """Write a Python obj as pretty JSON."""
    with open(filename, "w", encoding="utf8") as f:
        json.dump(data, f, indent=4, sort_keys=True, ensure_ascii=False)


def save_partial_scrobbles(tracks, filename=SCROBBLES_FILE):
    """Save scrobbles checkpoint."""
    save_json(tracks, filename)


def save_state(username, last_page, total_pages, tracks_count, filename=STATE_FILE):
    """Save progress info so we can resume later."""
    state = {
        "username": username,
        "last_page": last_page,
        "total_pages": total_pages,
        "tracks_count": tracks_count,
    }
    with open(filename, "w", encoding="utf8") as f:
        json.dump(state, f, indent=4, sort_keys=True)


def load_state(username, filename=STATE_FILE):
    """Load progress state if it exists and matches this user."""
    if not os.path.exists(filename):
        return None

    try:
        with open(filename, "r", encoding="utf8") as f:
            state = json.load(f)
    except Exception:
        return None

    if state.get("username") != username:
        return None

    return state


# main

if __name__ == '__main__':
    import config

    username = config.USERNAME
    api_key = config.API_KEY

    # dumping favs first (if not already dumped)
    if not os.path.exists(FAVOURITES_FILE):
        stderr.write("Fetching favourites (loved tracks)…\n")
        fav_total_pages = get_loved_pages(username, api_key)
        favourites = []

        fav_page = 1
        while fav_page <= fav_total_pages:
            stderr.write('\rfavourites page %d/%d %d%%' %
                         (fav_page, fav_total_pages, fav_page * 100 / fav_total_pages))

            loved_tracks = get_loved_tracks(username, api_key, fav_page)

            for track in loved_tracks:
                try:
                    artist_obj = track.get('artist', {})
                    artist_name = artist_obj.get('name') or artist_obj.get('#text')

                    fav = {
                        'artist': artist_name,
                        'name': track.get('name'),
                        'date': track.get('date', {}).get('uts'),
                        'mbid': track.get('mbid'),
                        'url': track.get('url'),
                    }
                    favourites.append(fav)
                except Exception as e:
                    logging.error('while processing favourite %s', track)
                    raise e

            fav_page += 1

        save_json(favourites, FAVOURITES_FILE)
        stderr.write("\nFavourites saved to %s (%d items)\n" %
                     (FAVOURITES_FILE, len(favourites)))
    else:
        stderr.write("Favourites file exists, skipping favourites download.\n")

    # then scrobbles
    stderr.write("Fetching scrobbles…\n")
    total = get_pages(username, api_key)

    # try to resume from previous state
    state = load_state(username)
    tracks = []
    start_page = 1

    if state and os.path.exists(SCROBBLES_FILE):
        last_page = state.get("last_page", 0)
        tracks_count = state.get("tracks_count", 0)

        if 0 < last_page < total:
            # load existing tracks from previous run
            try:
                with open(SCROBBLES_FILE, "r", encoding="utf8") as f:
                    tracks = json.load(f)
                if len(tracks) == tracks_count:
                    start_page = last_page + 1
                    stderr.write(
                        "Resuming from page %d (previously completed up to page %d, %d tracks).\n"
                        % (start_page, last_page, tracks_count)
                    )
                else:
                    stderr.write(
                        "State file and scrobbles.json mismatch; starting from scratch.\n"
                    )
            except Exception:
                stderr.write("Could not load existing scrobbles; starting from scratch.\n")
        else:
            stderr.write("Previous state indicates all pages or invalid; starting from scratch.\n")
    else:
        stderr.write("No previous state found; starting from scratch.\n")

    if not tracks:
        tracks = []

    curPage = start_page
    outfile = SCROBBLES_FILE

    while curPage <= total:
        stderr.write('\rpage %d/%d %d%%' %
                     (curPage, total, curPage * 100 / total))

        response = get_scrobbles(username, api_key, curPage)

        for track in response:
            try:
                tracks.append({
                    'artist': track['artist']['#text'],
                    'name': track['name'],
                    'album': track['album']['#text'],
                    'date': track['date']['uts'],
                })
            except Exception as e:
                if 'nowplaying' in str(track):
                    # currently playing: no date yet
                    pass
                else:
                    logging.error('while processing %s', track)
                    raise e

        # checkpoint every 10 pages or on the last page
        if (curPage % 10 == 0) or (curPage == total):
            save_partial_scrobbles(tracks, outfile)
            save_state(username, curPage, total, len(tracks))

        curPage += 1

    stderr.write("\nScrobbles saved to %s (%d items)\n" %
                 (outfile, len(tracks)))
