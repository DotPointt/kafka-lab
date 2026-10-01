// Режим UI. На локальном стенде — 'live' (данные от control-center).
// GitHub Actions при публикации на Pages заменяет этот файл на { mode: 'demo', repo: '<url>' }:
// статическая копия проигрывает запись стенда (demo/recording.json).
window.KAFKA_LAB = window.KAFKA_LAB || { mode: 'live' };
