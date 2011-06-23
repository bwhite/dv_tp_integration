hdfs_root=vid_gen/`date +%s`/
python video_frame_generator.py ${hdfs_root}/frame_input
hadoop jar dvintegration.jar com.dappervision.examples.DVIntegration ${hdfs_root}/frame_input ${hdfs_root}/video_output
python video_reader.py ${hdfs_root}/video_output