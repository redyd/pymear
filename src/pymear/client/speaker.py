from typing import cast

import pyttsx3
from elevenlabs import ElevenLabs
from elevenlabs import stream as el_stream


class NativeTTS:
    """Text-to-speech engine using pyttsx3."""

    def __init__(self, rate=160, volume=1.0, lang='fr'):
        self.engine = pyttsx3.init()
        self.voices = cast(list, self.engine.getProperty('voices'))

        for voice in self.voices:
            if "french" in voice.name.lower() or "fr" in voice.id.lower():
                self.engine.setProperty('voice', voice.id)
                break

        self.setRate(rate)
        self.setVolume(volume)

    def setRate(self, rate):
        self.engine.setProperty('rate', rate)

    def setVolume(self, volume):
        self.engine.setProperty('volume', volume)

    def say(self, text):
        self.engine.say(text)
        print(f"[TTS]: Saying '{text}'")
        self.engine.runAndWait()

class ElevenLabsTTS:
    """Text-to-speech engine using ElevenLabs Conversational AI."""

    def __init__(self, voice_id: str, api_key: str):
        if not api_key:
            raise ValueError("ELEVENLABS_API_KEY must be set")

        self.client = ElevenLabs(api_key=api_key)
        self.voice_id = voice_id


    def say(self, text: str):
        print(f"[ElevenLabs] Requête pour: {text}")
        audio_stream = self.client.text_to_speech.stream(
            text=text,
            voice_id=self.voice_id,
            model_id="eleven_multilingual_v2",
        )
        el_stream(audio_stream)
        print("[ElevenLabs] Lecture terminée")
