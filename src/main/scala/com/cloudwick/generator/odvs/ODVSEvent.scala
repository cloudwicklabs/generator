package com.cloudwick.generator.odvs

/**
 * Description goes here
 * @author ashrith 
 */
case class ODVSEvent(
  cId: Int,
  cName: String,
  userActive: Int,
  cWatchInit: Long,
  cWatchPauseTime: Long,
  cMovieRating: String,
  mId: String,
  mName: String,
  mReleaseDate: String,
  mLength: Int,
  mGenre: String)
