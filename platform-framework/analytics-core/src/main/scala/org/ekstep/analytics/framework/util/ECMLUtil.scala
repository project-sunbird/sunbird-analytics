package org.ekstep.analytics.framework.util

import org.apache.commons.lang3.StringUtils

/**
 * @author mahesh
 */

case class ECMLMedia(id: Option[String], src: Option[String], `type`: Option[String], assetId: Option[String], asset_id: Option[String]);
case class ECMLManifest(media: Option[List[ECMLMedia]]);
case class ECMLTheme(manifest: Option[ECMLManifest])
case class ECMLContent(theme: Option[ECMLTheme]);
object ECMLUtil {

	def getAssetIds(ecmlJson: String): List[String] = {
		val ecml = JSONUtils.deserialize[ECMLContent](ecmlJson);
		if (!ecml.theme.isEmpty) {
			val manifest = ecml.theme.get.manifest;
			if (!manifest.isEmpty) {
				val media = manifest.get.media
				media.getOrElse(List()).map { x => getAssetId(x) }.filter { x => StringUtils.isNotBlank(x) }
			} else {
				List();
			}
		} else {
			List();	
		}
	} 
	
	private def getAssetId(media: ECMLMedia) : String = {
		if (media.assetId.nonEmpty) 
			media.assetId.get
		else if (media.asset_id.nonEmpty)
			media.asset_id.get
		else if (media.id.nonEmpty)
			media.id.get
		else "";
	}
}