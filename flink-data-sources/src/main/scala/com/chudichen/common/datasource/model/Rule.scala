package com.chudichen.common.datasource.model

/**
 * 规则
 *
 * @author chudichen
 * @since 2020-11-19
 */
case class Rule(id: String, name: String, typeStr: String, measurement: String,
                expression: String, threshold: String, level: String, targetType: String,
                targetId: String, webhook: String)
