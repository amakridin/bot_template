def parse_json(data):
    ret = {}
    try:
        update_type = data['update_type']
        ret['update_type'] = update_type
        if update_type in ('message_callback', 'inline_keyboard'):
            ret['mid'] = data['message']['body']['mid']
            ret['text'] = data['message']['body']['text']
            ret['user_id'] = data['callback']['user']['user_id']
            ret['timestamp'] = data['callback']['timestamp']
            ret['user_name'] = data['callback']['user']['name']
            ret['callback_id'] = data['callback']['callback_id']
            ret['callback_payload'] = data['callback']['payload']
            ret['chat_id'] = data['message']['recipient']['chat_id']
            ret['chat_type'] = data['message']['recipient']['chat_type']
        elif update_type in ('user_removed', 'user_added', 'bot_added', 'bot_removed'):
            ret['chat_id'] = data['chat_id']
            ret['chat_type'] = 'chat'
            ret['timestamp'] = data['timestamp']
            ret['user_id'] = data['user']['user_id']
            ret['user_name'] = data['user']['name']
        elif update_type in ('message_created', 'message_edited'):
            ret['user_id'] = data['message']['sender']['user_id']
            ret['user_name'] = data['message']['sender']['name']
            ret['chat_id'] = data['message']['recipient']['chat_id']
            ret['chat_type'] = data['message']['recipient']['chat_type']  # dialog, chat
            ret['timestamp'] = data['message']['timestamp']
            ret['sender_id'] = data['message']['sender']['user_id']
            if ret['chat_type'] == 'dialog':
                ret['bot_id'] = data['message']['recipient']['user_id']
                if data['message']['body'].get('attachments') is not None:
                    if data['message']['body']['attachments'][0]['type'] == 'contact':
                        ret['attachment_userid'] = data['message']['body']['attachments'][0][
                            'payload']['tam_info']['user_id']
            ret['mid'] = data['message']['body']['mid']
            ret['text'] = data['message']['body']['text']
        elif update_type == 'message_removed':
            ret['mid'] = data['message_id']
        elif update_type == 'constructor':
            ret['session_id'] = data['session_id']
            ret['timestamp'] = int(data['timestamp'])
            ret['user_id'] = int(data['user_id'])
            ret['payload'] = data['payload']
            ret['input_type'] = data.get('input_type')
            ret['message'] = data.get('message')
        elif update_type == 'message_chat_created':
            ret['chat_id'] = data['chat']['chat_id']
            ret['title'] = data['chat']['title']
            title = ret['title']
            ret['link'] = data['chat']['link']
            ret['timestamp'] = data['timestamp']

        return ret
    except Exception as ex:
        print(ex.__str__())
        return {}
