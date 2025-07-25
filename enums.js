const MediaTypeEnum = {
  ARTIST: "artist",
  COLLECTION_PERIPHERAL: "collection-peripheral",
  EPISODE: "episode",
  EPISODE_PERIPHERAL: "episode-peripheral",
  REEL: "reel",
  SHOW_EPISODE: "show-episode",
  SHOW_PERIPHERAL: "show-peripheral",
};

const SourceTypeEnum = {
  GOOGLE_DRIVE: "google-drive",
  LOCAL_UPLOAD: "local-upload",
};

const MediaStatusEnum = {
  CREATED: "created",
  TRANSCODING_COMPLETED: "transcoding-completed",
  TRANSCODING_FAILED: "transcoding-failed",
  TRANSCODING_STARTED: "transcoding-started",
  UPLOAD_COMPLETED: "upload-completed",
  UPLOAD_FAILED: "upload-failed",
  UPLOADING: "uploading",
};

const TranscodingEngineEnum = {
  AWS_MEDIA_CONVERT: "aws-media-convert",
  VISIONULAR: "visionular",
};

const TranscodingTaskTypeEnum = {
  VIDEO_TRANSCODING: "video-transcoding",
};

const TaskStatusEnum = {
  COMPLETED: "completed",
  FAILED: "failed",
  IN_PROGRESS: "in-progress",
};

module.exports = {
  MediaTypeEnum,
  SourceTypeEnum,
  MediaStatusEnum,
  TranscodingEngineEnum,
  TranscodingTaskTypeEnum,
  TaskStatusEnum,
};
