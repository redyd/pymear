from typing import cast

import pyttsx3


class TextToSpeech:
    """Text-to-speech engine using pyttsx3."""

    def __init__(self, rate=160, volume=1.0):
        self.engine = pyttsx3.init()
        self.voices = cast(list, self.engine.getProperty('voices'))

        for voice in self.voices:
            if 'french' in voice.name.lower() or 'fr' in voice.id.lower():
                self.engine.setProperty('voice', voice.id)
                break

        self.set_rate(rate)
        self.set_volume(volume)

    def set_rate(self, rate):
        self.engine.setProperty('rate', rate)

    def set_volume(self, volume):
        self.engine.setProperty('volume', volume)

    def say(self, text):
        self.engine.say(text)
        print(f"[TTS]: Saying {len(text.split(' '))} words.")
        self.engine.runAndWait()

tts = TextToSpeech(140, 2)
tts.say("Bonjour ceci est un test en français.")
