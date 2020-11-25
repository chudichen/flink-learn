package com.chudichen.project.deduplication.model

/**
 * 用户访问网页的日志
 *
 * @author chudichen
 * @since 2020-11-24
 */
case class UserVisitWebEvent(id: String, date: String, pageId: Int, userId: String, url: String)
