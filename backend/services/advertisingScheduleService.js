function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function parseInteger(value, fallback = 0) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export const ADVERTISING_PRIORITY_ORDER = {
  low: 1,
  medium: 2,
  high: 3,
  very_high: 4
};

export const ADVERTISING_FREQUENCY_MODES = [
  'once',
  'daily',
  'weekly',
  'weekdays',
  'every_x_hours',
  'every_x_days'
];

export const WEEKDAY_CODES = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];

export function nowIso() {
  return new Date().toISOString();
}

export function normalizeAdvertisingStatus(value) {
  return cleanText(String(value || '')).toLowerCase() === 'active' ? 'active' : 'paused';
}

export function normalizeAdvertisingPriority(value) {
  const normalized = cleanText(String(value || '')).toLowerCase();
  return Object.prototype.hasOwnProperty.call(ADVERTISING_PRIORITY_ORDER, normalized) ? normalized : 'medium';
}

export function normalizeAdvertisingFrequencyMode(value) {
  const normalized = cleanText(String(value || '')).toLowerCase();
  return ADVERTISING_FREQUENCY_MODES.includes(normalized) ? normalized : 'daily';
}

export function normalizeDateValue(value) {
  const trimmed = cleanText(value);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return '';
  }

  const [year, month, day] = trimmed.split('-').map(Number);
  const date = new Date(year, month - 1, day);
  return Number.isNaN(date.getTime()) ? '' : trimmed;
}

export function normalizeTimeEntries(value, fallback = ['09:00']) {
  const sourceValues = Array.isArray(value)
    ? value
    : typeof value === 'string'
      ? value.split(/[\n,;]+/)
      : fallback;

  const unique = Array.from(
    new Set(
      sourceValues
        .map((item) => cleanText(String(item)))
        .filter((item) => /^\d{2}:\d{2}$/.test(item))
        .filter((item) => {
          const [hours, minutes] = item.split(':').map(Number);
          return hours >= 0 && hours <= 23 && minutes >= 0 && minutes <= 59;
        })
    )
  );

  return (unique.length ? unique : fallback).sort();
}

export function normalizeWeekdayEntries(value, fallback = []) {
  const sourceValues = Array.isArray(value)
    ? value
    : typeof value === 'string'
      ? value.split(/[\n,;]+/)
      : fallback;

  return Array.from(
    new Set(
      sourceValues
        .map((item) => cleanText(String(item)).toLowerCase())
        .map((item) => {
          if (WEEKDAY_CODES.includes(item)) {
            return item;
          }

          const numeric = parseInteger(item, -1);
          return numeric >= 0 && numeric <= 6 ? WEEKDAY_CODES[numeric] : '';
        })
        .filter(Boolean)
    )
  );
}

export function parseLocalDate(dateValue) {
  const normalized = normalizeDateValue(dateValue);
  if (!normalized) {
    return null;
  }

  const [year, month, day] = normalized.split('-').map(Number);
  const date = new Date(year, month - 1, day, 0, 0, 0, 0);
  return Number.isNaN(date.getTime()) ? null : date;
}

export function formatDateKey(dateValue = new Date()) {
  const date = dateValue instanceof Date ? new Date(dateValue) : new Date(dateValue);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

export function startOfLocalDay(dateValue = new Date()) {
  const date =
    typeof dateValue === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(dateValue)
      ? parseLocalDate(dateValue)
      : dateValue instanceof Date
        ? new Date(dateValue)
        : new Date(dateValue);
  if (!date) {
    return new Date();
  }
  date.setHours(0, 0, 0, 0);
  return date;
}

export function endOfLocalDay(dateValue = new Date()) {
  const date = startOfLocalDay(dateValue);
  date.setHours(23, 59, 59, 999);
  return date;
}

export function shiftLocalDays(dateValue, days) {
  const date = dateValue instanceof Date ? new Date(dateValue) : new Date(dateValue);
  date.setDate(date.getDate() + days);
  return date;
}

export function combineDateAndTime(dateValue, timeValue) {
  const baseDate = typeof dateValue === 'string' ? parseLocalDate(dateValue) : startOfLocalDay(dateValue);
  if (!baseDate) {
    return null;
  }

  const [hours, minutes] = normalizeTimeEntries([timeValue])[0].split(':').map(Number);
  baseDate.setHours(hours, minutes, 0, 0);
  return baseDate;
}

export function getWeekdayCode(dateValue) {
  const date = dateValue instanceof Date ? dateValue : new Date(dateValue);
  return WEEKDAY_CODES[date.getDay()] || 'sun';
}

export function getPriorityWeight(priority) {
  return ADVERTISING_PRIORITY_ORDER[normalizeAdvertisingPriority(priority)] || ADVERTISING_PRIORITY_ORDER.medium;
}

function isOccurrenceWithinDateRange(module, occurrence) {
  const startDate = parseLocalDate(module.startDate);
  if (!startDate) {
    return false;
  }

  const endDate = module.endDate ? endOfLocalDay(module.endDate) : null;
  if (occurrence.getTime() < startDate.getTime()) {
    return false;
  }

  if (endDate && occurrence.getTime() > endDate.getTime()) {
    return false;
  }

  return true;
}

function getDayDifference(startDate, candidateDate) {
  const start = startOfLocalDay(startDate).getTime();
  const candidate = startOfLocalDay(candidateDate).getTime();
  return Math.floor((candidate - start) / (24 * 60 * 60 * 1000));
}

function buildOccurrencesForSimpleTimes(module, dayDate) {
  return module.times
    .map((timeValue) => combineDateAndTime(dayDate, timeValue))
    .filter(Boolean)
    .filter((occurrence) => isOccurrenceWithinDateRange(module, occurrence));
}

function buildOccurrencesForHourlyMode(module, dayDate) {
  const intervalHours = Math.max(1, parseInteger(module.intervalHours, 6));
  const intervalMs = intervalHours * 60 * 60 * 1000;
  const dayStart = startOfLocalDay(dayDate);
  const dayEnd = endOfLocalDay(dayDate);
  const occurrences = [];

  module.times.forEach((timeValue) => {
    const anchor = combineDateAndTime(module.startDate, timeValue);
    if (!anchor || anchor.getTime() > dayEnd.getTime()) {
      return;
    }

    let nextOccurrence = new Date(anchor);
    if (nextOccurrence.getTime() < dayStart.getTime()) {
      const offsetMs = dayStart.getTime() - nextOccurrence.getTime();
      const steps = Math.ceil(offsetMs / intervalMs);
      nextOccurrence = new Date(nextOccurrence.getTime() + steps * intervalMs);
    }

    while (nextOccurrence.getTime() <= dayEnd.getTime()) {
      if (isOccurrenceWithinDateRange(module, nextOccurrence)) {
        occurrences.push(new Date(nextOccurrence));
      }

      nextOccurrence = new Date(nextOccurrence.getTime() + intervalMs);
    }
  });

  return occurrences;
}

export function listOccurrencesForDay(module, dayDate) {
  if (normalizeAdvertisingStatus(module.status) !== 'active') {
    return [];
  }

  const startDate = parseLocalDate(module.startDate);
  if (!startDate) {
    return [];
  }

  const candidateDay = startOfLocalDay(dayDate);
  const diffDays = getDayDifference(startDate, candidateDay);
  if (diffDays < 0) {
    return [];
  }

  const mode = normalizeAdvertisingFrequencyMode(module.frequencyMode);

  if (mode === 'every_x_hours') {
    return buildOccurrencesForHourlyMode(module, candidateDay).sort((left, right) => left.getTime() - right.getTime());
  }

  if (mode === 'once' && formatDateKey(candidateDay) !== formatDateKey(startDate)) {
    return [];
  }

  if (mode === 'weekly' && diffDays % 7 !== 0) {
    return [];
  }

  if (mode === 'weekdays' && !module.weekdays.includes(getWeekdayCode(candidateDay))) {
    return [];
  }

  if (mode === 'every_x_days') {
    const intervalDays = Math.max(1, parseInteger(module.intervalDays, 1));
    if (diffDays % intervalDays !== 0) {
      return [];
    }
  }

  return buildOccurrencesForSimpleTimes(module, candidateDay).sort((left, right) => left.getTime() - right.getTime());
}

export function listOccurrencesForRange(modules = [], fromDate = new Date(), days = 30) {
  const occurrences = [];

  for (let offset = 0; offset <= days; offset += 1) {
    const currentDay = shiftLocalDays(startOfLocalDay(fromDate), offset);
    modules.forEach((module) => {
      listOccurrencesForDay(module, currentDay).forEach((occurrence) => {
        if (occurrence.getTime() >= fromDate.getTime()) {
          occurrences.push({
            moduleId: module.id,
            moduleName: module.moduleName,
            priority: module.priority,
            priorityWeight: getPriorityWeight(module.priority),
            scheduledFor: occurrence,
            scheduledDateKey: formatDateKey(occurrence),
            status: module.status
          });
        }
      });
    });
  }

  return occurrences.sort((left, right) => {
    if (left.scheduledFor.getTime() !== right.scheduledFor.getTime()) {
      return left.scheduledFor.getTime() - right.scheduledFor.getTime();
    }

    if (left.priorityWeight !== right.priorityWeight) {
      return right.priorityWeight - left.priorityWeight;
    }

    return left.moduleId - right.moduleId;
  });
}

export function isSameMinute(leftDate, rightDate) {
  return (
    leftDate.getFullYear() === rightDate.getFullYear() &&
    leftDate.getMonth() === rightDate.getMonth() &&
    leftDate.getDate() === rightDate.getDate() &&
    leftDate.getHours() === rightDate.getHours() &&
    leftDate.getMinutes() === rightDate.getMinutes()
  );
}
