/*
 *  Copyright 2017-2026 Adobe.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.adobe.testing.s3mock.dto

/**
 * Case-insensitive lookup of an enum entry by [Enum.name].
 * Returns `null` when [value] is `null` or no entry matches.
 */
inline fun <reified T : Enum<T>> enumFromName(value: String?): T? = enumValues<T>().firstOrNull { it.name.equals(value, ignoreCase = true) }

/**
 * Case-insensitive lookup of an enum entry using [selector] to extract the comparison value.
 * Use when the serialised value differs from [Enum.name] (e.g. `"Directory"` instead of `DIRECTORY`).
 * Returns `null` when [value] is `null` or no entry matches.
 */
inline fun <reified T : Enum<T>> enumFromValue(
  value: String?,
  selector: (T) -> String,
): T? = enumValues<T>().firstOrNull { selector(it).equals(value, ignoreCase = true) }
