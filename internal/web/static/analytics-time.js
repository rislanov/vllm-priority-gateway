(() => {
  'use strict';

  const pad = (value, width = 2) => String(value).padStart(width, '0');
  function instant(canonical) {
    const date = new Date(canonical);
    if (!Number.isFinite(date.getTime())) throw new RangeError('Invalid timestamp');
    return date;
  }

  function parts(date, timeZone) {
    const formatter = new Intl.DateTimeFormat('en-GB', {
      timeZone, calendar: 'gregory', numberingSystem: 'latn', hourCycle: 'h23',
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit', timeZoneName: 'longOffset',
    });
    return Object.fromEntries(formatter.formatToParts(date).map(({type, value}) => [type, value]));
  }

  function offset(date, timeZone) {
    const name = parts(date, timeZone).timeZoneName;
    return name === 'GMT' ? 'UTC+00:00' : name.replace('GMT', 'UTC');
  }

  function formatTimestamp(canonical, {timeZone, precision = 'millisecond'} = {}) {
    const date = instant(canonical);
    const p = parts(date, timeZone);
    const minute = `${p.year.padStart(4, '0')}-${p.month}-${p.day} ${p.hour}:${p.minute}`;
    const seconds = precision === 'minute' ? '' : `:${p.second}.${pad(date.getUTCMilliseconds(), 3)}`;
    return `${minute}${seconds} ${offset(date, timeZone)}`;
  }

  function formatAxisTimestamp(canonical, timeZone) {
    const p = parts(instant(canonical), timeZone);
    return `${p.year.padStart(4, '0')}-${p.month}-${p.day} ${p.hour}:${p.minute}`;
  }

  function toLocalInput(utcMinuteValue) {
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(utcMinuteValue)) throw new RangeError('Invalid UTC input');
    const date = instant(`${utcMinuteValue}:00Z`);
    return `${pad(date.getFullYear(), 4)}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
  }

  function parseLocalInput(value) {
    const error = 'Enter a valid local date and time. Times skipped by a clock change do not exist.';
    const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/.exec(value);
    if (!match) return {error};
    const [year, month, day, hour, minute] = match.slice(1).map(Number);
    // setFullYear avoids Date's special constructor treatment of years 0–99.
    // Native local-time setters select the earlier instant in a repeated hour.
    const date = new Date(0);
    date.setFullYear(year, month - 1, day);
    date.setHours(hour, minute, 0, 0);
    if (date.getFullYear() !== year || date.getMonth() !== month - 1 || date.getDate() !== day
      || date.getHours() !== hour || date.getMinutes() !== minute) return {error};
    return {canonical: date.toISOString(), offsetLabel: offset(date)};
  }

  function resolveBoundary(value, initialValue, originalCanonical) {
    if (value === initialValue) {
      // Do not serialize Date here: the canonical string can carry nanoseconds
      // and can identify the later occurrence of a repeated local time.
      return {canonical: originalCanonical, offsetLabel: offset(instant(originalCanonical))};
    }
    return parseLocalInput(value);
  }

  window.LLMGWAnalyticsTime = Object.freeze({formatTimestamp, formatAxisTimestamp, toLocalInput, parseLocalInput, resolveBoundary});
})();
