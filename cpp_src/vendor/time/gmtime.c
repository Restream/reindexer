/*
 * Copyright (C) 2001-2003 by egnite Software GmbH. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holders nor the names of
 *    contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY EGNITE SOFTWARE GMBH AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL EGNITE
 * SOFTWARE GMBH OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * For additional information see http://www.ethernut.de/
 *
 * Portions of the following functions are derived from material which is
 * Copyright (c) 1985 by Microsoft Corporation.  All rights are reserved.
 */
/*
 * $Log$
 * Revision 1.8  2008/08/11 06:59:40  haraldkipp
 * BSD types replaced by stdint types (feature request #1282721).
 *
 * Revision 1.7  2005/08/02 17:46:47  haraldkipp
 * Major API documentation update.
 *
 * Revision 1.6  2004/10/14 16:43:00  drsung
 * Fixed compiler warning "comparison between signed and unsigned"
 *
 * Revision 1.5  2003/12/19 22:26:37  drsung
 * Dox written.
 *
 * Revision 1.4  2003/11/27 09:17:18  drsung
 * Wrong comment fixed
 *
 * Revision 1.3  2003/11/26 12:45:20  drsung
 * Portability issues ... again
 *
 * Revision 1.2  2003/11/26 11:13:17  haraldkipp
 * Portability issues
 *
 * Revision 1.1  2003/11/24 18:07:37  drsung
 * first release
 *
 *
 */

#include <stdint.h>
#include <time.h>

#include <stddef.h>
#define _DAY_SEC (24UL * 60UL * 60UL)	 /* secs in a day */
#define _YEAR_SEC (365L * _DAY_SEC)		  /* secs in a year */
#define _FOUR_YEAR_SEC (1461L * _DAY_SEC) /* secs in a 4 year interval */
#define _DEC_SEC 315532800UL			  /* secs in 1970-1979 */
#define _BASE_YEAR 70L					  /* 1970 is the base year */
#define _BASE_DOW 4						  /* 01-01-70 was a Thursday */
#define _LEAP_YEAR_ADJUST 17L			  /* Leap years 1900 - 1970 */
#define _MAX_YEAR 138L					  /* 2038 is the max year */

/*      static arrays used by gmtime to determine date and time
 *       values. Shows days from being of year.
 ***************************************************************/
int _lpdays[] = {-1, 30, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
int _days[] = {-1, 30, 58, 89, 119, 150, 180, 211, 242, 272, 303, 333, 364};

int fast_gmtime_r(const time_t *timer, struct tm *ptm) {
	time_t ctimer = *timer; /* var to calculate with */
	uint8_t isleapyear = 0; /* current year is leap year */
	uint32_t tmptimer;
	int *mdays; /* pointer to _numdayslp or _numdays */

	if (!ptm) /* check pointer */
		return -1;

	/*
	   First calculate the number of four-year-interval, so calculation
	   of leap year will be simple. Btw, because 2000 IS a leap year and
	   2100 is out of range, this formula is so simple.
	 */
	tmptimer = (uint32_t)(ctimer / _FOUR_YEAR_SEC);
	ctimer -= ((time_t)tmptimer * _FOUR_YEAR_SEC);

	/* Determine the correct year within the interval */
	tmptimer = (tmptimer * 4) + 70; /* 1970, 1974, 1978,... */
	if (ctimer >= (time_t)_YEAR_SEC) {
		tmptimer++; /* 1971, 1975, 1979,... */
		ctimer -= _YEAR_SEC;
		if (ctimer >= (time_t)_YEAR_SEC) {
			tmptimer++; /* 1972, 1976, 1980,... (all leap years!) */
			ctimer -= _YEAR_SEC;
			/* A leap year has 366 days, so compare to _YEAR_SEC + _DAY_SEC */
			if (ctimer >= (time_t)(_YEAR_SEC + _DAY_SEC)) {
				tmptimer++; /* 1973, 1977, 1981,... */
				ctimer -= (_YEAR_SEC + _DAY_SEC);
			} else
				isleapyear = 1; /*If leap year, set the flag */
		}
	}

	/*
	   tmptimer now has the value for tm_year. ctimer now holds the
	   number of elapsed seconds since the beginning of that year.
	 */
	ptm->tm_year = tmptimer;

	/*
	   Calculate days since January 1st and store it to tm_yday.
	   Leave ctimer with number of elapsed seconds in that day.
	 */
	ptm->tm_yday = (int)(ctimer / _DAY_SEC);
	ctimer -= (time_t)(ptm->tm_yday) * _DAY_SEC;

	/*
	   Determine months since January (Note, range is 0 - 11)
	   and day of month (range: 1 - 31)
	 */
	if (isleapyear)
		mdays = _lpdays;
	else
		mdays = _days;

	for (tmptimer = 1; mdays[tmptimer] < ptm->tm_yday; tmptimer++)
		;

	ptm->tm_mon = --tmptimer;

	ptm->tm_mday = ptm->tm_yday - mdays[tmptimer];

	/* Calculate day of week. Sunday is 0 */
	ptm->tm_wday = ((int)(*timer / _DAY_SEC) + _BASE_DOW) % 7;

	/* Calculate the time of day from the remaining seconds */
	ptm->tm_hour = (int)(ctimer / 3600);
	ctimer -= (time_t)ptm->tm_hour * 3600L;

	ptm->tm_min = (int)(ctimer / 60);
	ptm->tm_sec = (int)(ctimer - (ptm->tm_min) * 60);

	ptm->tm_isdst = 0;
	return 0;
}
