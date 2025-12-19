#!/usr/bin/env python3

import json
import urllib.error
import urllib.request
from sys import stdout, stderr
import logging
import os
import time

__author__ = 'Alexander Popov'
__version__ = '1.2.0'
__license__ = 'Unlicense'


STATE_FILE = "scrobbles_state.json"
SCROBBLES_FILE = "scrobbles.json"
FAVOURITES_FILE = "favourites.json"


# API helpers

def _get(url, max_attempts=5):
    """Simple helper to GET and decode JSON with basic retries."""
    delay = 1
    last_exc = None
    for _ in range(max_attempts):
        try:
            resp = urllib.request.urlopen(url).read().decode("utf8")
            return json.loads(resp)
        except urllib.error.HTTPError as exc:
            last_exc = exc
            time.sleep(delay)
            delay = min(delay * 2, 60)
    if last_exc:
        raise last_exc
    raise RuntimeError("Request failed for URL: %s" % url)


def _format_ts(ts):
    """Return a human-friendly UTC string for a unix timestamp."""
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(int(ts)))
    except Exception:
        return str(ts)


def _build_recent_url(username, api_key, page=None, from_ts=None, to_ts=None):
    query_parts = [
        "method=user.getrecenttracks",
        "user={0}".format(username),
        "api_key={0}".format(api_key),
        "format=json",
        "limit=200",
    ]
    if page is not None:
        query_parts.append("page={0}".format(page))
    if from_ts:
        query_parts.append("from={0}".format(int(from_ts)))
    if to_ts:
        query_parts.append("to={0}".format(int(to_ts)))
    return "https://ws.audioscrobbler.com/2.0/?" + "&".join(query_parts)


def get_pages(username, api_key, from_ts=None, to_ts=None):
    """Total pages for recent tracks (scrobbles)."""
    data = _get(_build_recent_url(username, api_key, from_ts=from_ts, to_ts=to_ts))
    return int(data['recenttracks']['@attr']['totalPages'])


def get_scrobbles(username, api_key, page, from_ts=None, to_ts=None):
    data = _get(_build_recent_url(username, api_key, page=page, from_ts=from_ts, to_ts=to_ts))
    return data['recenttracks']['track']


def get_loved_pages(username, api_key):
    """Total pages for loved tracks (favourites)."""
    data = _get(
        'https://ws.audioscrobbler.com/2.0/'
        '?method=user.getlovedtracks&user={0}&api_key={1}&format=json'
        '&limit=1000'.format(username, api_key)
    )
    return int(data['lovedtracks']['@attr']['totalPages'])


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


def save_state(username, last_page, total_pages, tracks_count, resume_to_ts=None, filename=STATE_FILE):
    """Save progress info so we can resume later."""
    state = {
        "username": username,
        "last_page": last_page,
        "total_pages": total_pages,
        "tracks_count": tracks_count,
        "resume_to_ts": resume_to_ts,
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

    # then scrobbles (timestamp-based resume)
    stderr.write("Fetching scrobbles…\n")

    state = load_state(username) or {}
    tracks = []
    seen = set()
    resume_to_ts = None

    if os.path.exists(SCROBBLES_FILE):
        try:
            with open(SCROBBLES_FILE, "r", encoding="utf8") as f:
                tracks = json.load(f)
            for t in tracks:
                key = (
                    t.get('artist'),
                    t.get('name'),
                    t.get('album'),
                    t.get('date'),
                )
                seen.add(key)
            if tracks and tracks[-1].get('date'):
                resume_to_ts = int(tracks[-1]['date']) - 1
                stderr.write(
                    "Resuming scrobble download; already have %d tracks down to %s (%s).\n"
                    % (len(tracks), tracks[-1]['date'], _format_ts(tracks[-1]['date']))
                )
            else:
                stderr.write("Existing scrobbles.json found but missing dates; starting fresh.\n")
                tracks = []
                seen = set()
        except Exception:
            stderr.write("Could not load existing scrobbles; starting from scratch.\n")
            tracks = []
            seen = set()
    else:
        stderr.write("No previous scrobbles found; starting from scratch.\n")

    total = get_pages(username, api_key, to_ts=resume_to_ts)

    cur_page = 1
    outfile = SCROBBLES_FILE

    while cur_page <= total:
        stderr.write('\rpage %d/%d %d%%' %
                     (cur_page, total, cur_page * 100 / total))

        response = get_scrobbles(username, api_key, cur_page, to_ts=resume_to_ts)

        for track in response:
            try:
                key = (
                    track['artist']['#text'],
                    track['name'],
                    track['album']['#text'],
                    track['date']['uts'],
                )
                if key in seen:
                    continue

                tracks.append({
                    'artist': track['artist']['#text'],
                    'name': track['name'],
                    'album': track['album']['#text'],
                    'date': track['date']['uts'],
                })
                seen.add(key)
            except Exception as e:
                if 'nowplaying' in str(track):
                    # currently playing: no date yet
                    pass
                else:
                    logging.error('while processing %s', track)
                    raise e

        # checkpoint every 10 pages or on the last page
        if (cur_page % 10 == 0) or (cur_page == total):
            newest_to_oldest = tracks
            oldest_ts = int(newest_to_oldest[-1]['date']) if newest_to_oldest else None
            save_partial_scrobbles(tracks, outfile)
            save_state(username, cur_page, total, len(tracks), oldest_ts)

        cur_page += 1

    stderr.write("\nScrobbles saved to %s (%d items)\n" %
                 (outfile, len(tracks)))
