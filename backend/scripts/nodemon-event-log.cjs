const eventType = process.argv[2] || '';
const timestamp = new Date().toISOString();

if (eventType === 'restart') {
  console.log('[FILE_CHANGE_DETECTED]', {
    detectedAt: timestamp
  });
}
