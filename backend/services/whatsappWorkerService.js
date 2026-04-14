export async function processWhatsappPublishingTarget(target, queuePayload) {
  const text = queuePayload.textByChannel?.whatsapp || queuePayload.title || '';
  if (!text.trim()) {
    throw new Error('WhatsApp Payload ohne Text kann nicht verarbeitet werden.');
  }

  const imageUrl =
    target.image_source === 'standard'
      ? queuePayload.imageVariants?.standard || ''
      : target.image_source === 'upload'
        ? queuePayload.imageVariants?.upload || ''
        : '';

  return {
    status: 'simulated-post',
    textLength: text.length,
    imageUrl: imageUrl || null
  };
}
